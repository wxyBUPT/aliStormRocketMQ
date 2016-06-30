#!/bin/sh

echo "Start time is $1"
echo "End time is $2"
echo "Dir name is $3"

for dir in `ls $3`
do
    if [ -e $3/$dir ]
    then
        echo "$3/$dir is a file"
    fi
done


