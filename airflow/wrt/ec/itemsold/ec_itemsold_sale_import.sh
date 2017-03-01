#!/usr/bin/env bash
source ~/.bashrc
lastday=$1

hive<<EOF
use wl_base;
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds=$lastday);
EOF


