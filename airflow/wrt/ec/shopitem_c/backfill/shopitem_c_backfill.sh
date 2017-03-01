#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
lastday=$1
last_2_days=$2
table=wl_base.t_base_ec_shopitem_c
hive<<EOF
LOAD DATA  INPATH '/user/wrt/shopitem_c_tmp' OVERWRITE INTO TABLE $table PARTITION (ds='0temp');
EOF



