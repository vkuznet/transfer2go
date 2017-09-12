#!/bin/bash
# Author: Valentin Kuznetsov < vkuznet [] gmail () com>

if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ]; then
    echo "Usage: run.sh"
    echo "Perform transfer2go test among 3 agents: main|source|destination"
    echo "Test creates dummy file; register it in source agent; place request to main agent"
    echo "approve request in main agent and transfer the file from source to destination"
    exit
fi
exe=./transfer2go
tdir=$PWD/test
schema=$PWD/static/sql/sqlite3/schema.sql

export X509_USER_KEY=~/.globus/userkey.pem
export X509_USER_CERT=~/.globus/usercert.pem

# helper function to kill certain process
pskill ()
{
    local pid;
    pid=$(ps ax | grep -i $1 | grep -v grep | awk '{ print $1 }' | tr '\n' ' ');
    if [ -n "$pid" ]; then
        echo -n "killing $1: $pid...";
        kill -9 $pid;
        echo "slaughtered.";
    fi
}
# helper function to show certain process
psgrep ()
{
    ps axu | grep -v grep | grep "$@" -i --color=auto
}

wdir=/tmp/transfer2go
if [ -d $wdir ]; then
    rm -rf $wdir
fi
echo "create agents areas in $wdir"
mkdir -p $wdir/{catalog,config,model,main,source,destination}
cat > $wdir/records.json << EOF
[
    {"lfn":"file.root",
     "pfn":"$wdir/source/file.root",
     "block":"/a/b/c#123",
     "dataset":"/a/b/c"}
]
EOF

echo "Create config and catalogs"
cat > $wdir/config/main.json << EOF
{
    "catalog":"$wdir/catalog/main.json",
    "protocol":"http",
    "backend":"$wdir/main",
    "tool":"/bin/cp",
    "url":"http://localhost:8989",
    "mfile":"mainAgentMetrics.log",
    "csvfile":"$wdir/model/history.csv",
    "minterval":60,
    "verbose":0,
    "name":"mainAgent",
    "staticdir":"static",
    "type":"pull",
    "MonitorTime":3600,
    "trinterval": "24h",
    "router":true
}
EOF
cat > $wdir/catalog/main.json << EOF
{
    "type":"sqlite3",
    "uri":"$wdir/catalog/main.db"
}
EOF
cat $wdir/config/main.json | \
    sed -e "s,main,source,g" -e "s,8989,8000,g" -e "s,pull,push,g" -e "s,true,false,g" \
    > $wdir/config/source.json 
cat $wdir/config/main.json | \
    sed -e "s,main,destination,g" -e "s,8989,9000,g" -e "s,pull,push,g" -e "s,true,false,g"\
    > $wdir/config/destination.json 
cat $wdir/catalog/main.json | sed -e "s,main,source,g" > $wdir/catalog/source.json 
cat $wdir/catalog/main.json | sed -e "s,main,destination,g" > $wdir/catalog/destination.json 
sqlite3 $wdir/catalog/main.db < $schema
sqlite3 $wdir/catalog/source.db < $schema
sqlite3 $wdir/catalog/destination.db < $schema

mainlog=main.log
srclog=src.log
dstlog=dst.log

mainconfig=$wdir/config/main.json
srcconfig=$wdir/config/source.json
dstconfig=$wdir/config/destination.json

mainAgentName=`cat $mainconfig | python -c "import sys, json; print json.load(sys.stdin)['name']"`
srcAgentName=`cat $srcconfig | python -c "import sys, json; print json.load(sys.stdin)['name']"`
dstAgentName=`cat $dstconfig | python -c "import sys, json; print json.load(sys.stdin)['name']"`

mainAgentUrl=`cat $mainconfig | python -c "import sys, json; print json.load(sys.stdin)['url']"`
srcAgentUrl=`cat $srcconfig | python -c "import sys, json; print json.load(sys.stdin)['url']"`
dstAgentUrl=`cat $dstconfig | python -c "import sys, json; print json.load(sys.stdin)['url']"`

echo "main agent: $mainAgentName ($mainAgentUrl)"
echo "source agent: $srcAgentName ($srcAgentUrl)"
echo "destination agent: $dstAgentName ($dstAgentUrl)"

echo "create large file in source area"
dd if=/dev/zero of=$wdir/source/file.root bs=1024 count=0 seek=1024
# 100MB file
#dd if=/dev/zero of=$wdir/source/file.root bs=1024 count=0 seek=$[1024*100]
# 1GB file
#dd if=/dev/zero of=$wdir/source/file.root bs=1G count=0 seek=1G

set -e

trap 'kill %1; kill %2; kill %3' ERR EXIT

echo "Kill previous transfer2go processes (if any)"
pskill transfer2go

echo "Start $mainAgentName at $mainAgentUrl"
$exe -config $mainconfig -auth=false > $mainlog 2>&1 &

sleep 1

echo "Start $srcAgentName at $srcAgentUrl"
$exe -config $srcconfig -auth=false -agent $mainAgentUrl > $srclog 2>&1 &

sleep 1

echo "Start $dstAgentName at $dstAgentUrl"
$exe -config $dstconfig -auth=false -agent $mainAgentUrl > $dstlog 2>&1 &

sleep 1

echo
echo "Test setup is done:"
echo
echo "status of $mainAgentName"
curl $mainAgentUrl/status
echo
echo "status of $srcAgentName"
curl $srcAgentUrl/status
echo
echo "status of $dstAgentName"
curl $dstAgentUrl/status
echo
psgrep transfer2go
echo
echo "upload records into source agent"
echo "`cat $wdir/records.json`"
$exe -agent $srcAgentUrl -register=$wdir/records.json 
echo
echo "list records at $mainAgentUrl"
curl $mainAgentUrl/tfc
echo
echo "list records at $srcAgentUrl"
curl $srcAgentUrl/tfc
echo
echo "list records at $dstAgentUrl"
curl $dstAgentUrl/tfc
echo
echo "Transfer file from $srcAgentName ($srcAgentUrl) to $dstAgentName ($dstAgentUrl)"
$exe -agent=$mainAgentUrl -src=${srcAgentName}:file.root -dst=$dstAgentName -verbose 1
echo
echo "list known requests on $mainAgentName"
requests=`curl -s "$mainAgentUrl/list?type=pending"`
echo $requests
rid=`echo $requests | python -c "import sys, json; print(json.load(sys.stdin)[0]['id'])"`
echo
echo "You may visit $mainAgentUrl/html/main.html to view and/or approve requests"
echo
echo "approve request id:$rid"
$exe -agent=$mainAgentUrl -approve=$rid
sleep 2
echo
if cmp -s "$wdir/source/file.root" "$wdir/destination/file.root"
then
    echo "The files match"
    exit 0
else
    echo "The files at source and destination are different"
    echo "list all files in $wdir"
    ls -alR $wdir
    exit 1
fi
