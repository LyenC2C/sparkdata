#!/usr/bin/env bash
source ~/.bashrc

lastday=$1

hive<<EOF
set hive.merge.mapfiles= true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=16777216;
LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_shopitem_b PARTITION (ds=$lastday);
EOF
