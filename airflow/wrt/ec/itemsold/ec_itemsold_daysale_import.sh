#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
last_2_days=$1


hive<<EOF
use wl_base;
LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds=last_2_days);
EOF

