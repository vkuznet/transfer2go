#!/bin/sh

set -e

trap 'kill %1; kill %2' ERR EXIT

cmd=./transfer2go

args=${1+"$@"}

touch supervisor.log

echo "Your supervisor is started checkout supervisor.log"

service(){
  for (( ; ; )); do
    local pid=`ps auxwww | egrep "$cmd" | grep -v grep | awk 'BEGIN{ORS=" "} {print $2}'`
    echo "PID=$pid" >> supervisor.log
    if [ -z "$pid" ]; then
      local tstamp=`date "+%Y/%m/%d %H:%M:%S"`
      echo "$tstamp goserver is not running, restart" >> supervisor.log
      $cmd $args >> supervisor.log 2>&1
      if [ "$args" == "-help" ] || [ "$args" == "--help" ]; then
        echo "Pass -auth and -config arguments"
        echo "[Example]: bash supervisor.sh -auth=false -config=test/config/config1.json"
        break
      fi
    fi
    sleep 10
  done
}
service
