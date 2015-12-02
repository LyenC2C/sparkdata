#!/usr/bin/env bash

source ~/.bashrc

#table=$1

#path=/hive/warehouse/wlbase_dev.db/$table


path=$1
local_tmp_path=/mnt/pzz/hdfs_merge_tmp
#hadoop fs -ls $path

echo $path

merge_day="`date +%Y%m%d`"

hadoop fs -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)==8) print "ds="$NF }' >${local_tmp_path}/ds_log

hadoop fs -ls $path  |sed  '1d' |awk -F"ds="  '{if(length($NF)!=8) print "ds="$NF }' >${local_tmp_path}/ds_log_error


echo 'deal file merger'
cat ${local_tmp_path}/ds_log
while read line
do    
    #256*1024*1024 268435456

    #cat
    echo "cat target file from hdfs"
    hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'|xargs -t -I {} hadoop fs -cat {} > ${local_tmp_path}/${merge_day}-0000

    #rm hdfs
    echo "rm hdfs file"
    hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'|xargs -t -I {} hadoop fs -rm {}
    #hadoop fs -ls $path/$line/ |grep -v '^d'|sed '1d'|awk '{if($5<268435456){print $NF}}'

    #put
    echo "put local file ${merge_day}-0000 to hdfs ${path}/${line}"
    hadoop fs -put ${local_tmp_path}/${merge_day}-0000  $path/$line/

    #rm local
    rm ${local_tmp_path}/${merge_day}-0000
    
done<${local_tmp_path}/ds_log

cat ${local_tmp_path}/ds_log_error
while read line
do
    #echo 'test,do not remove'
    hadoop fs -rm $path/$line/*
done<${local_tmp_path}/ds_log_error


