#!/usr/bin/env bash
inputdir=$1
filedate=$2
table=$3
hadoop fs -mv ${inputdir}/$filedate  /hive/warehouse/wlbase_dev.db/$table/ds=$filedate/$filedate_`date +%s`
#echo "-cp ${inputdir}/${1}_`date +%s`  /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_test/ds=$1/"