#!/usr/bin/env bash
inputdir=$2
ds=`echo ${inputdir##*/} | grep -E -o '[0-9]{8}'`
filedate=$1
table=$3
ts=`date +%s`
cmd="hadoop fs -mv ${inputdir}/$filedate  /hive/warehouse/wlbase_dev.db/$table/ds=$filedate/${filedate}_$ds_$ts"
echo $cmd
$cmd
#echo "-cp ${inputdir}/${1}_`date +%s`  /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_test/ds=$1/"


