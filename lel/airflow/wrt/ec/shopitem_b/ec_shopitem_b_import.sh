#!/usr/bin/env bash
source ~/.bashrc

zuotian=$(date -d '1 days ago' +%Y%m%d)

hive<<EOF
use wlbase_dev;
set hive.merge.mapredfiles = true;
LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_b PARTITION (ds='$zuotian');
EOF
