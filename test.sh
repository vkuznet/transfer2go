#!/bin/bash

set -e

trap 'kill %1; kill %2' ERR EXIT

./transfer2go -config test/config/config1.json -auth=false >/dev/null 2>&1 &

sleep 1

./transfer2go -config test/config/config2.json -auth=false -agent http://localhost:8989 >/dev/null 2>&1 &

sleep 1

cd test && go test
