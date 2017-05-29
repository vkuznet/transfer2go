#!/bin/bash

echo "Time taken to insert the data"
time sqlite3 test.db < data.sql

echo "Time taken to query"
time sqlite3 test.db << EOF >/dev/null 2>&1
_datasets = select * from datasets;
_blocks = select * from blocks where datasetid=(select id from _datasets where dataset="/41Z/6Ik/KAy");
select * from files where blockid=(select id from _blocks where block="/41Z/6Ik/KAy#tfKr");
select * from files as F join blocks as B on F.blockid=B.id join datasets as D ON F.datasetid = D.id where d.dataset="/41Z/6Ik/KAy";
EOF
