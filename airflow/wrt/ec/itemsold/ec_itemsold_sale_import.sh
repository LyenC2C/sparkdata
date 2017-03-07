#!/usr/bin/env bash
source ~/.bashrc
lastday=$1

hive<<EOF
set hive.merge.mapfiles= true;
set hive.merge.mapredfiles= true;
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_item_sold_dev PARTITION (ds=$lastday);
EOF



