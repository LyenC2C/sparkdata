#!/usr/bin/env bash

source ~/.bashrc

#table=$1

#path=/hive/warehouse/wlbase_dev.db/$table


path=$1
local_tmp_path=/mnt/pzz/hdfs_merge_tmp
#hadoop fs -ls $path

echo $path

merge_day="`date +%Y%m%d`"


hadoop fs  -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)==8) print "ds="$NF }' >ds_log

hadoop fs -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)!=8) print "ds="$NF }' >ds_log_error


echo 'deal file merger'

while read line
do
    echo  "hadoop fs -cat $path/$line/* > part-00000"
    hadoop fs -cat $path/$line/* > ${local_tmp_path}/${merge_day}-0000

    echo "hadoop fs -rm $path/$line/part-*"
    hadoop fs -rm $path/$line/*

    echo  "hadoop fs -put part-00000  $path/$line/"
    hadoop fs -put ${local_tmp_path}/${merge_day}-0000  $path/$line/
    rm ${local_tmp_path}/${merge_day}-000
done<ds_log

while read line
do
    hadoop fs -rm $path/$line/*
done<ds_log_error