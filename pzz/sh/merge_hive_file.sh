#!/usr/bin/env bash

source ~/.bashrc

#table=$1

#path=/hive/warehouse/wlbase_dev.db/$table


path=$1

#hadoop fs -ls $path

echo $path



hadoop fs  -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)==8) print "ds="$NF }' >ds_log

hadoop fs -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)!=8) print "ds="$NF }' >ds_log_error


echo 'deal file merger'

while read line
do
    echo  "hadoop fs -cat $path/$line/* > part-00000"
    hadoop fs -cat $path/$line/* > part-00000

    echo "hadoop fs -rm $path/$line/part-*"
    hadoop fs -rm $path/$line/*

    echo  "hadoop fs -put part-00000  $path/$line/"
    hadoop fs -put part-00000  $path/$line/
    rm part-00000
done<ds_log

while read line
do
    hadoop fs -rm $path/$line/
done<ds_log_error