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
cat ds_log
while read line
do    
    #256*1024*1024 268435456

    #cat
    hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'|xargs -t -I {} hadoop fs -cat {} > ${local_tmp_path}/${merge_day}-0001
    #put
    hadoop fs -put ${local_tmp_path}/${merge_day}-0001  $path/$line/
    #rm
    #hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'|xargs -t -I {} hadoop fs -rm {}
    hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'

    rm ${local_tmp_path}/${merge_day}-0001
    
done<ds_log

cat ds_log_error
while read line
do
    echo "test,don't remove"
    #hadoop fs -rm $path/$line/*
done<ds_log_error


