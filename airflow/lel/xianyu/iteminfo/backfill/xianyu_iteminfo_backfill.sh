#!/bin/bash

source ~/.bashrc
lastday=$(date -d '1 days ago' +%Y%m%d)
last_2_days=$(date -d '2 days ago' +%Y%m%d)
table=wl_base.t_base_ec_xianyu_iteminfo

hadoop fs -test -e /user/lel/temp/xianyu_iteminfo
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/xianyu_iteminfo
else
    echo 'Directory is not exist,you can run your spark as you want!!!'
fi

spark-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 $work/spark/xianyu/xianyu_iteminfo.py $lastday


#闲鱼iteminfo——日更——当天处理的是昨天的数据



hive<<EOF
LOAD DATA  INPATH '/user/lel/temp/xianyu_iteminfo' OVERWRITE INTO TABLE $table PARTITION (ds='00tmp');
set hive.merge.mapfiles= true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=16777216;
insert OVERWRITE table $table PARTITION(ds = $lastday)
select
case when t1.itemid is null then t2.itemid else t1.itemid end,
case when t1.itemid is null then t2.userid else t1.userid end,
case when t1.itemid is null then t2.phone else t1.phone end,
case when t1.itemid is null then t2.contacts else t1.contacts end,
case when t1.itemid is null then t2.title else t1.title end,
case when t1.itemid is null then t2.province else t1.province end,
case when t1.itemid is null then t2.city else t1.city end,
case when t1.itemid is null then t2.area else t1.area end,
case when t1.itemid is null then t2.auctionType else t1.auctionType end,
case when t1.itemid is null then t2.description else t1.description end,
case when t1.itemid is null then t2.detailFrom else t1.detailFrom end,
case when t1.itemid is null then t2.favorNum else t1.favorNum end,
case when t1.itemid is null then t2.commentNum else t1.commentNum end,
case when t1.itemid is null then t2.firstModified else t1.firstModified end,
case when t1.itemid is null then t2.firstModifiedDiff else t1.firstModifiedDiff end,
case when t1.itemid is null then t2.t_from else t1.t_from end,
case when t1.itemid is null then t2.gps else t1.gps end,
case when t1.itemid is null then t2.offline else t1.offline end,
case when t1.itemid is null then t2.originalPrice else t1.originalPrice end,
case when t1.itemid is null then t2.price else t1.price end,
case when t1.itemid is null then t2.postprice else t1.postprice end,
case when t1.itemid is null then t2.userNick else t1.userNick end,
case when t1.itemid is null then t2.categoryid else t1.categoryid end,
case when t1.itemid is null then t2.categoryName else t1.categoryName end,
case when t1.itemid is null then t2.fishPoolid else t1.fishPoolid end,
case when t1.itemid is null then t2.fishpoolName else t1.fishpoolName end,
case when t1.itemid is null then t2.bar else t1.bar end,
case when t1.itemid is null then t2.barInfo else t1.barInfo end,
case when t1.itemid is null then t2.abbr else t1.abbr end,
case when t1.itemid is null then t2.zhima else t1.zhima end,
case when t1.itemid is null then t2.shiren else t1.shiren end,
case when t1.itemid is null then t2.ts else t1.ts end
from
(select * from  $table where ds = '00tmp')t1
full outer JOIN
(select * from $table where ds = $last_2_days)t2
ON
t1.itemid = t2.itemid;
EOF