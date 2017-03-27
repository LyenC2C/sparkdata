#!/bin/bash
date=`hadoop fs -ls /hive/warehouse/wl_base.db/t_wrt_shixin_qiye | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
echo $date