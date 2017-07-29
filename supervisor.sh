#!/bin/sh

set -e

trap 'kill %1' ERR EXIT

cmd=$1
args=${@:2}
echo "Your supervisor is started."

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit 1
fi

service(){
  for (( ; ; )); do
    local pid=`ps auxwww | egrep "$cmd" | grep -v -e bash -e grep | awk 'BEGIN{ORS=" "} {print $2}'`
    echo "PID=$pid"
    if [ -z "$pid" ]; then
      local tstamp=`date "+%Y/%m/%d %H:%M:%S"`
      echo "$tstamp goserver is not running, restart"
      $cmd $args &
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
