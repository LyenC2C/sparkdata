#!/bin/bash
source ~/.bashrc
date=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_item_search | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
echo $date