#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

#ds=$(date -d '2 days ago' +%Y%m%d)

hive<<EOF

use wlbase_dev;

LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds=$1);

EOF

