#!/usr/bin/env bash
source ~/.bashrc

lastday=$1

hive<<EOF
use wlbase_dev;
set hive.merge.mapfiles = true;
LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_b PARTITION (ds='$lastday');
EOF
