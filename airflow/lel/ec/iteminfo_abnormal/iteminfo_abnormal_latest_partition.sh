#!/bin/bash
date=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_item_abnormal | awk -F '=' '{print $2}' | sort -r |awk 'NR==1{print $0}'`
echo $date