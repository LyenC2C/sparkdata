#!/usr/bin/env bash

source ~/.bashrc

#table=$1

#path=/hive/warehouse/wlbase_dev.db/$table


path=$1
local_tmp_path=/mnt/pzz/hdfs_merge_tmp
#hadoop fs -ls $path

echo $path

merge_day="`date +%Y%m%d`"

hadoop fs -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)==8) print "ds="$NF }' >ds_log

hadoop fs -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)!=8) print "ds="$NF }' >ds_log_error


echo 'deal file merger'

while read line
do
    echo  "hadoop fs -cat $path/$line/* > ${merge_day}-0000"
    #256*1024*1024 268435456
    #grep except the floder
    #cat
    hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'|xargs -I {} hadoop fs -cat $path/$line/{} > ${local_tmp_path}/${merge_day}-0000
    echo "hadoop fs -rm $path/$line/part-*"

    hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'|xargs -I {} hadoop fs -rm $path/$line/{} 
    echo  "hadoop fs -put ${merge_day}-0000 $path/$line/"
    hadoop fs -put ${local_tmp_path}/${merge_day}-0000  $path/$line/
    rm ${local_tmp_path}/${merge_day}-0000
done<ds_log

while read line
do
    hadoop fs -rm $path/$line/*
done<ds_log_error


