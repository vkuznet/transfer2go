#!/bin/bash

set -e

trap 'kill %1; kill %2; kill %3' ERR EXIT

./transfer2go -config test/config/main.json -auth=false >/dev/null 2>&1 &

sleep 1

./transfer2go -config test/config/source.json -auth=false -agent http://localhost:8989 >/dev/null 2>&1 &

sleep 1

./transfer2go -config test/config/destination.json -auth=false -agent http://localhost:8989 >/dev/null 2>&1 &

sleep 1

cd test && go test function_test.go
