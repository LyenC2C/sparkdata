#!/usr/bin/env bash
source ~/.bashrc

zuotian=$(date -d '1 days ago' +%Y%m%d)

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds='$zuotian');
EOF


