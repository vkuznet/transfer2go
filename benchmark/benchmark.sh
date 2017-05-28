#!/bin/bash

echo "Time taken to insert the data"
time sqlite3 test.db < data.sql

echo "Time taken to query"
time sqlite3 test.db "select * from datasets where id=59;" >/dev/null 2>&1 \
&& sqlite3 test.db "select * from blocks where datasetid=59;" >/dev/null 2>&1 \
&& sqlite3 test.db "select * from blocks where datasetid=59 AND id=59066;" >/dev/null 2>&1 \
&& sqlite3 test.db "select * from blocks where datasetid=59 AND id=59033;" >/dev/null 2>&1 \
&& sqlite3 test.db "select * from files where datasetid=59 AND id=59033 AND id=59033004;" >/dev/null 2>&1 \
&& sqlite3 test.db "select * from files where datasetid=59 AND id=59033 AND id=59033019;" >/dev/null 2>&1 \
&& sqlite3 test.db "select * from files where lfn=\"/1GN/XBy/iFl-/1GN/XBy/iFl#XCAo-y6F4v.root\";" >/dev/null 2>&1
