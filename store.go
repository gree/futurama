package futurama

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"github.com/satori/go.uuid"
	"strconv"
	"strings"
	"time"
)

const (
	SQL_TMPL_CREATE_DATABASE = `CREATE DATABASE IF NOT EXISTS %s`
	SQL_TMPL_CREATE_TABLE    = `CREATE TABLE IF NOT EXISTS %s (
 id VARCHAR(128) NOT NULL,
 trigger_type VARCHAR(64),
 trigger_time DATETIME%s NOT NULL,
 retry_attempts INT DEFAULT 0,
 data TEXT,
 status INT,
 owner VARCHAR(64) NOT NULL DEFAULT '',
 owner_lock_time DATETIME%s DEFAULT NULL,
 owner_seq BIGINT NOT NULL DEFAULT 0,
 time_created DATETIME%s,
 PRIMARY KEY(id))`
)

var (
	SQL_SAVE_EVENT             string
	SQL_DELETE_EVENT           string
	SQL_UPDATE_EVENT_STATUS    string
	SQL_UPDATE_EVENT_FOR_RETRY string

	SQL_RESET_DELAYED_EVENTS string
	SQL_DECLARE_OWNERSHIP    string
	SQL_SELECT_EVENTS        string
)

func openMySQL(cfg *MySQLConfig) (*sql.DB, error) {
	suf := ""
	if cfg.MySQL6 {
		suf = "(6)"
	}
	sqlCreateDb := fmt.Sprintf(SQL_TMPL_CREATE_DATABASE, cfg.DbName)
	sqlCreateTable := fmt.Sprintf(SQL_TMPL_CREATE_TABLE, cfg.TableName, suf, suf, suf)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Pass, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(sqlCreateDb); err != nil {
		return nil, err
	}
	db.Close()

	dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.DbName)
	glog.Infof("Open mysql: %s", dsn)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(sqlCreateTable); err != nil {
		return nil, err
	}
	return db, nil
}

type MySQLStore struct {
	cfg        *MySQLConfig
	timeWindow time.Duration

	db *sql.DB

	nbError    Seq32
	nbSave     Seq32
	nbCancel   Seq32
	nbComplete Seq32
	nbRetry    Seq32
	nbReset    Seq32
}

func NewMySQLStore(cfg *Config) *MySQLStore {
	SQL_SAVE_EVENT = fmt.Sprintf(`INSERT INTO %s
 (id, trigger_type, trigger_time, data, status, time_created)
 VALUES (?, ?, ?, ?, ?, NOW())`, cfg.TableName)
	SQL_DELETE_EVENT = fmt.Sprintf(`DELETE FROM %s WHERE id=?`, cfg.TableName)
	SQL_UPDATE_EVENT_STATUS = fmt.Sprintf(`UPDATE %s SET status=? WHERE id=?`, cfg.TableName)
	SQL_UPDATE_EVENT_FOR_RETRY = fmt.Sprintf(`UPDATE %s SET
 owner='', owner_lock_time=NULL, owner_seq=0,
 trigger_time=?, retry_attempts=? WHERE id=?`, cfg.TableName)

	// used by consumer
	SQL_RESET_DELAYED_EVENTS = fmt.Sprintf(`UPDATE %s SET owner='', owner_lock_time=NULL WHERE
   owner != '' AND owner_lock_time < SUBDATE( NOW(), INTERVAL %d SECOND )`,
		cfg.TableName, cfg.ConsumerLockTimeoutSec)

	SQL_DECLARE_OWNERSHIP = fmt.Sprintf(`UPDATE %s SET owner=?, owner_lock_time=NOW(), owner_seq=? WHERE
   id < ? AND owner = '' LIMIT %d`, cfg.TableName, cfg.ConsumerSelectLimit)
	SQL_SELECT_EVENTS = fmt.Sprintf(`SELECT id, trigger_type, trigger_time, retry_attempts, data, status
 FROM %s WHERE id < ? AND owner=? AND (owner_seq=? or status=%d)`, cfg.TableName, EventStatus_CANCEL)

	return &MySQLStore{
		cfg:        &cfg.MySQLConfig,
		timeWindow: time.Duration(cfg.ConsumerTimeWindowSec) * time.Second,
	}
}

func (self *MySQLStore) GetDb() *sql.DB {
	return self.db
}

func (self *MySQLStore) Open() error {
	if db, err := openMySQL(self.cfg); err != nil {
		glog.Errorln("Open:", err)
		return err
	} else {
		self.db = db
		self.db.SetMaxOpenConns(self.cfg.MaxOpenConnection)
	}
	return nil
}

func (self *MySQLStore) Close() {
	glog.Infoln("Close")
	if self.db != nil {
		self.db.Close()
	}
}

func (self *MySQLStore) Save(ev *Event) string {
	ev.Id = fmt.Sprintf("%d_%s", ev.TriggerTime.Unix(), uuid.NewV1().String())
	glog.Infoln("Save", ev)
	self.nbSave.Next()

	jsonBytes, _ := Encoder.Marshal(ev.Data)
	evData := strings.TrimSpace(string(jsonBytes))

	_, err := self.db.Exec(SQL_SAVE_EVENT,
		ev.Id,
		ev.TriggerType,
		ev.TriggerTime,
		evData,
		ev.Status,
	)
	if err != nil {
		glog.Errorln("Save:", err)
		self.nbError.Next()
		return ""
	}
	return ev.Id
}

func (self *MySQLStore) Cancel(evId string) error {
	glog.Infoln("Cancel", evId)
	self.nbCancel.Next()

	return self.updateEventStatus(evId, EventStatus_CANCEL)
}

func (self *MySQLStore) UpdateStatus(evId string, status EventStatus) error {
	glog.Infoln("UpdateStatus", evId, status)
	self.nbComplete.Next()
	return self.deleteEvent(evId)
}

func (self *MySQLStore) UpdateForRetry(ev *Event, retryParam interface{}) error {
	glog.Infoln("UpdateForRetry", ev.Id, ev.TriggerTime, ev.Attempts)
	self.nbRetry.Next()

	_, err := self.db.Exec(SQL_UPDATE_EVENT_FOR_RETRY, ev.TriggerTime, ev.Attempts, ev.Id)
	if err != nil {
		glog.Errorln("UpdateForRetry:", err, ev.Id)
		self.nbError.Next()
		return err
	}
	return nil
}

func (self *MySQLStore) deleteEvent(id string) error {
	if _, err := self.db.Exec(SQL_DELETE_EVENT, id); err != nil {
		glog.Errorln("deleteEvent:", err, id)
		self.nbError.Next()
		return err
	}
	if glog.V(2) {
		glog.Infoln("deleteEvent", id)
	}
	return nil
}

func (self *MySQLStore) updateEventStatus(id string, status EventStatus) error {
	if _, err := self.db.Exec(SQL_UPDATE_EVENT_STATUS, status, id); err != nil {
		glog.Errorln("updateEventStatus:", err, id)
		self.nbError.Next()
		return err
	}
	if glog.V(2) {
		glog.Infoln("updateEventStatus", id, status)
	}
	return nil
}

func (self *MySQLStore) resetDelayedEvents(ownerId string) error {
	if glog.V(2) {
		glog.Infoln("resetDelayedEvents", ownerId)
	}
	self.nbReset.Next()
	if res, err := self.db.Exec(SQL_RESET_DELAYED_EVENTS); err != nil {
		glog.Errorln("Reset delayed events:", err, ownerId)
		self.nbError.Next()
		return err
	} else {
		rowsAffected, _ := res.RowsAffected()
		if rowsAffected > 0 {
			glog.Warningln("Reset delayed events:", rowsAffected, ownerId)
		}
	}
	return nil
}

func (self *MySQLStore) getEvents(seq int32, ownerId string) (err error, events []*Event) {
	__begin := time.Now()
	err = nil
	events = nil
	// declare ownership
	upperTimeStamp := time.Now().Add(self.timeWindow).Unix()
	upperId := strconv.FormatInt(upperTimeStamp, 10)
	_, err = self.db.Exec(SQL_DECLARE_OWNERSHIP, ownerId, seq, upperId)
	if err != nil {
		glog.Errorln("Declare ownership:", err, ownerId)
		self.nbError.Next()
		return
	}
	// get events
	rows, errQuery := self.db.Query(SQL_SELECT_EVENTS, upperId, ownerId, seq)
	if errQuery != nil {
		err = errQuery
		return
	}
	defer rows.Close()

	var strData string
	for rows.Next() {
		ev := &Event{}
		errScan := rows.Scan(
			&ev.Id,
			&ev.TriggerType,
			&ev.TriggerTime,
			&ev.Attempts,
			&strData,
			&ev.Status,
		)
		if errScan != nil {
			err = errScan
			return
		}

		var data interface{}
		decoder := json.NewDecoder(strings.NewReader(strData))
		decoder.UseNumber()
		decoder.Decode(&data)

		ev.Data = data
		events = append(events, ev)
	}

	errRows := rows.Err()
	if errRows != nil {
		err = errRows
		return
	}

	du := time.Since(__begin)
	if len(events) > 0 {
		glog.Infof("GetEvents %s seq: %d took: %dus", ownerId, seq, du.Nanoseconds())
	}
	return
}

func (self *MySQLStore) GetStat(reset bool) map[string]interface{} {
	stat := map[string]interface{}{
		"nbError":    self.nbError.Get(),
		"nbSave":     self.nbSave.Get(),
		"nbCancel":   self.nbCancel.Get(),
		"nbComplete": self.nbComplete.Get(),
		"nbRetry":    self.nbRetry.Get(),
		"nbReset":    self.nbReset.Get(),
	}
	if reset {
		self.nbError.Reset()
		self.nbSave.Reset()
		self.nbCancel.Reset()
		self.nbComplete.Reset()
		self.nbRetry.Reset()
		self.nbReset.Reset()
	}

	return stat
}

// For testing ONLY
func TestOnly_ResetDb(cfg *MySQLConfig) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Pass, cfg.Host, cfg.Port)
	glog.Infoln("Drop database", cfg.DbName, dsn)
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", cfg.DbName)); err != nil {
		glog.Errorf("Drop db, err: %s", err)
	}
}

func TestOnly_SelectEvents(cfg *MySQLConfig) []*Event {
	SELECT_SQL := fmt.Sprintf(`SELECT id, trigger_type, trigger_time, retry_attempts, status, owner
 FROM %s`, cfg.TableName)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.DbName)
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()

	events := make([]*Event, 0)
	rows, err := db.Query(SELECT_SQL)
	if err != nil {
		glog.Errorln("TestSelectEvents err:", err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		ev := &Event{}
		if err := rows.Scan(
			&ev.Id,
			&ev.TriggerType,
			&ev.TriggerTime,
			&ev.Attempts,
			&ev.Status,
			&ev.Owner,
		); err != nil {
			glog.Errorln("TestSelectEvents err:", err)
			return nil
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		glog.Errorln("TestSelectEvents err:", err)
		return nil
	}

	glog.Infoln("TestSelectEvents", len(events))
	return events
}
