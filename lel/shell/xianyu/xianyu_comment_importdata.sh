#!/bin/bash
source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
last_2_days=$(date -d '2 days ago' +%Y%m%d)

hadoop fs -test -e /user/lel/temp/xianyu_comment_2016
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/xianyu_comment_2016
else
    echo 'Directory is not exist Or Zero bytes in size'
fi

spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 80 ~/wolong/sparkdata/lel/spark/xianyu/t_xianyu_comment.py $lastday


hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/lel/temp/xianyu_comment_2016' OVERWRITE INTO TABLE wlbase_dev.t_base_ec_xianyu_item_comment PARTITION (ds='tmp');

insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_item_comment PARTITION(ds = $lastday)
select
case when t1.itemid is null then t2.itemid else t1.itemid end,
case when t1.itemid is null then t2.commentId else t1.commentId end,
case when t1.itemid is null then t2.content else t1.content end,
case when t1.itemid is null then t2.reportTime else t1.reportTime end,
case when t1.itemid is null then t2.reporterName else t1.reporterName end,
case when t1.itemid is null then t2.reporterNick else t1.reporterNick end,
case when t1.itemid is null then t2.ts else t1.ts end
from
(select * from  wlbase_dev.t_base_ec_xianyu_item_comment where ds = 'tmp')t1
full outer JOIN
(select * from wlbase_dev.t_base_ec_xianyu_item_comment where ds = $last_2_days)t2
ON
t1.itemid = t2.itemid;
EOF
