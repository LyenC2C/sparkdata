#!/bin/bash
source ~/.bashrc
date
date  +%Y%m%d

today=$(date -d '0 days ago' +%Y%m%d)
last_7_days=$(date -d '7 days ago' +%Y%m%d)

hive <<EOF
use wlbase_dev;
insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_itemid_update PARTITION(ds = $today)
select t1.itemid as itemid from
(select itemid,commentnum from wlbase_dev.t_base_ec_xianyu_iteminfo where ds= $today and commentnum > 0 ) t1
left join
(select itemid,commentnum from wlbase_dev.t_base_ec_xianyu_iteminfo where ds=$last_7_days and commentnum > 0 ) t2
on t1.itemid = t2.itemid
where t1.commentnum <> t2.commentnum or t2.itemid is null;
EOF

