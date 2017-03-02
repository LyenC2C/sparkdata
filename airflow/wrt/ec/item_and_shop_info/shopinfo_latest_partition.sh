#!/bin/bash
date=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
echo $date