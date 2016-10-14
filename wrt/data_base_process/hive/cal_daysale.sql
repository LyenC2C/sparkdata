#!/usr/bin/env bash
ds=$1

hive<<EOF

use wlbase_dev;

LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds='$ds');

EOF