#!/bin/bash

start=`(date +%s%N)`
sqlite3 test.db < data.sql
end=`(date +%s%N)`
echo "Time takes to Insert 1000 documents in nano seconds"
echo "expr $end - $start"

start=`(date +%s%N)`
sqlite3 test.db "select * from datasets where id=327;" >/dev/null 2>&1 &
end=`(date +%s%N)`
echo "Time takes to search document from datasets table in nano seconds"
echo "Query: select * from datasets where id=327;"
echo "expr $end - $start"
