#!/bin/bash

#闲鱼itemid——周更——当天处理的是上周的数据,today分区不包含当天数据
source ~/.bashrc
date
date  +%Y%m%d

today=$(date -d '0 days ago' +%Y%m%d)
lastday=$(date -d '1 days ago' +%Y%m%d)
last_7_days=$(date -d '7 days ago' +%Y%m%d)

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
use wl_base;
set hive.merge.mapredfiles = true;
insert OVERWRITE table wl_base.t_base_ec_xianyu_itemid_update PARTITION(ds = $today)
select t1.itemid as itemid from
(select itemid,commentnum from wl_base.t_base_ec_xianyu_iteminfo where ds= $lastday and commentnum > 0 ) t1
left join
(select itemid,commentnum from wl_base.t_base_ec_xianyu_iteminfo where ds=$last_7_days and commentnum > 0 ) t2
on t1.itemid = t2.itemid
where t1.commentnum <> t2.commentnum or t2.itemid is null;
EOF

