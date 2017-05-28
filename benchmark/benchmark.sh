#!/bin/bash

echo "Time taken to insert the data"
time sqlite3 test.db < data.sql
end=`(date +%s%N)`

echo "Time taken to query"
time sqlite3 test.db "select * from datasets where id=327;" >/dev/null 2>&1
