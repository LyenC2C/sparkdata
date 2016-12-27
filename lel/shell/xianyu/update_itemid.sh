#!/bin/bash
source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
thedaybeforelastday=$(date -d '2 days ago' +%Y%m%d)

hive <<EOF
use wlbase_dev;
insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_itemid_update PARTITION(ds = $lastday)
select t1.itemid as itemid from
(select itemid,commentnum from wlbase_dev.t_base_ec_xianyu_iteminfo where ds= $lastday and commentnum > 0 ) t1
left join
(select itemid,commentnum from wlbase_dev.t_base_ec_xianyu_iteminfo where ds=$thedaybeforelastday and commentnum > 0 ) t2
on t1.itemid = t2.itemid
where t1.commentnum > t2.commentnum or t2.itemid is null
EOF