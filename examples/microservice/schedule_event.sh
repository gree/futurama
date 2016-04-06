#!/bin/bash
# usage:
# scheduler_event.sh $port

WORK_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# schedule_event $port $delay_sec
schedule_event () {
    TRIGGER_TIME=$(($(date +%s) + $2))
    RES=$(cat $WORK_DIR/postdata.template.json | \
        sed "s/%trigger_time%/$TRIGGER_TIME/" | \
        sed "s/%port%/$1/" | \
        curl -d @- http://localhost:$1/add)
    echo $RES
}

# cancel_event $port $event_id
cancel_event () {
    curl http://localhost:$1/remove?event_id=$2
}

# ######
# main
echo "Trigger an event after 3sec"
schedule_event $1 3

sleep 4

echo "Trigger an event after 10sec"
RES=$(schedule_event $1 10)
EVENT_ID=$(echo $RES | cut -c14-60)

echo "=== EVENT CREATED ==="
echo $EVENT_ID
echo "==="

sleep 2

echo ""
echo "Cancel event $EVENT_ID"
cancel_event $1 $EVENT_ID

sleep 1


