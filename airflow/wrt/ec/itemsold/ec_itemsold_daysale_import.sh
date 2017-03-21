#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
last_2_days=$1


hive<<EOF
LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_item_daysale_dev_new PARTITION (ds=$last_2_days);
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=201326592;
set hive.support.quoted.identifiers=none;
insert OVERWRITE table wl_base.t_base_ec_item_daysale_dev_new PARTITION(ds = $last_2_days)
select \`(ds)?+.+\` from wl_base.t_base_ec_item_daysale_dev_new where ds = $last_2_days;
EOF

