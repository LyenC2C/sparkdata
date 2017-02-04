#!/bin/bash
source ~/.bashrc
date
date  +%Y%m%d

hadoop fs -test -e /user/lel/temp/xianyu_itemcomment
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/xianyu_itemcomment
else
    echo 'Directory is not exist Or Zero bytes in size'
fi

size=`hadoop fs -count -q /user/lel/temp/xianyu_itemcomment | awk '{print $7}'`

spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 80 ~/wolong/sparkdata/lel/spark/xianyu/xianyu_itemcomment.py $1

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/lel/temp/xianyu_comment_2016' OVERWRITE INTO TABLE wlbase_dev.t_base_ec_xianyu_item_comment PARTITION (ds='0000tmp');

insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_item_comment PARTITION(ds = $1)
select
case when t1.itemid is null then t2.itemid else t1.itemid end,
case when t1.itemid is null then t2.commentId else t1.commentId end,
case when t1.itemid is null then t2.content else t1.content end,
case when t1.itemid is null then t2.reportTime else t1.reportTime end,
case when t1.itemid is null then t2.reporterName else t1.reporterName end,
case when t1.itemid is null then t2.reporterNick else t1.reporterNick end,
case when t1.itemid is null then t2.ts else t1.ts end
from
(select * from  wlbase_dev.t_base_ec_xianyu_item_comment where ds = '0000tmp')t1
full outer JOIN
(select * from wlbase_dev.t_base_ec_xianyu_item_comment where ds = $2)t2
ON
t1.itemid = t2.itemid;
EOF
