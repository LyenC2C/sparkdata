#!/bin/bash
date_b=`hadoop fs -ls /hive/warehouse/wlbase_dev.db/t_base_ec_shopitem_b | awk -F '=' '{print $2}' | sort -r |awk 'NR==1{print $0}'`
date_c=`hadoop fs -ls /hive/warehouse/wlbase_dev.db/t_base_ec_shopitem_c | awk -F '=' '{print $2}' | sort -r |awk 'NR==1{print $0}'`
echo $date_b    $date_c