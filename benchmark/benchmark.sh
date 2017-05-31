#!/bin/bash

echo "Time taken to insert the data"
time sqlite3 test.db < data.sql

echo "Time taken to query"

begin=$(date +%s) 

max_1=3
max_2=3
i=1
j=1

output=`sqlite3 test.db "select * from datasets;"`


for d_row in ${output[@]}
do
	if (( $i > $max_1 ))
	then
		break
	fi
	
	j=0;
	dataset="$(cut -d'|' -f2 <<<"$d_row")"
    blocks=`sqlite3 test.db "select * from blocks as B JOIN datasets as D on B.datasetid=D.id where dataset=\"$dataset\";"`
    
    for b_row in ${blocks[@]}
	do
		if (( $j > $max_2 ))
		then
			break
		fi
		block="$(cut -d'|' -f2 <<<"$b_row")"
    	files=`sqlite3 test.db "select * from files as F JOIN blocks as B on F.blockid=B.id where block=\"$block\";"`
    	j=$((j+1))
	done
	i=$((i+1))
done

end=$(date +%s)
tottime=$(expr $end - $begin)

echo $tottime
