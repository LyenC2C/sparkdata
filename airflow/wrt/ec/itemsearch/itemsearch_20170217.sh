#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
table=wl_base.t_base_item_search

hadoop fs -test -e /user/wrt/temp/itemsearch
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/temp/itemsearch
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 10G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/itemsearch/t_base_item_search.py 20170217

hive<<EOF
use wl_base;
insert overwrite table wl_base.t_base_item_search partition(ds = '0temp')
SELECT
nid,max(user_id),max(comment_count),max(encryptedUserId),max(nick)
FROM
wl_base.t_base_item_search where ds = '0temp' group by nid;
EOF

hive<<EOF
use wlbase_dev;
insert OVERWRITE table $table PARTITION(ds = 20170216)
select
case when t1.nid is null then t2.nid else t1.nid end,
case when t1.nid is null then t2.user_id else t1.user_id end,
case when t1.nid is null then t2.comment_count else t1.comment_count end,
case when t1.nid is null then t2.encryptedUserId else t1.encryptedUserId end,
case when t1.nid is null then t2.nick else t1.nick end
from
(select * from  $table where ds = '0temp')t1
full outer JOIN
(select * from $table where ds = 20170207)t2
ON
t1.nid = t2.nid;
EOF