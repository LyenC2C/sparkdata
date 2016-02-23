#!/usr/bin/env bash
inputdir=$1
hadoop fs -mv ${inputdir}/$2  /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_test/ds=$2/$2_`date +%s`
#echo "-cp ${inputdir}/${1}_`date +%s`  /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_test/ds=$1/"