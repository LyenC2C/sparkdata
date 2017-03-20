#!/usr/bin/env bash
source ~/.bashrc

lastday=$1

hive<<EOF
LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_shopitem_b PARTITION (ds=$lastday);
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=201326592;
set hive.support.quoted.identifiers=none;
insert OVERWRITE table wl_base.t_base_ec_shopitem_b PARTITION(ds = $lastday)
select ``(ds)?+.+`` from wl_base.t_base_ec_shopitem_b where ds = $lastday;
EOF
