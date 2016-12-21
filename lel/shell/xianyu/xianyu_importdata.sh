today=$(date  +%Y%m%d)
hadoop fs -test -e /user/lel/temp/xianyu_2016
if [ $? -eq 0 ] ;then
    hdfs -fs  -rmr /user/lel/temp/xianyu_2016
else
    echo 'Directory is not exist Or Zero bytes in size'
fi

spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
~/wolong/sparkdata/lel/spark/xianyu/t_xianyu.py $ds

hive<<EOF

use wlbase_dev;
LOAD DATA  INPATH '/user/lel/temp/xianyu_2016' OVERWRITE INTO TABLE wlbase_dev.t_base_ec_xianyu_iteminfo PARTITION (ds='tmp');

insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_iteminfo PARTITION(ds = $today)
select
case when t1.id is null then t2.id else t1.id end,
case when t1.id is null then t2.userId else t1.userId end,
case when t1.id is null then t2.title else t1.title end,
case when t1.id is null then t2.province else t1.province end,
case when t1.id is null then t2.city else t1.city end,
case when t1.id is null then t2.area else t1.area end,
case when t1.id is null then t2.auctionType else t1.auctionType end,
case when t1.id is null then t2.description else t1.description end,
case when t1.id is null then t2.detailFrom else t1.detailFrom end,
case when t1.id is null then t2.favorNum else t1.favorNum end,
case when t1.id is null then t2.commentNum else t1.commentNum end,
case when t1.id is null then t2.firstModified else t1.firstModified end,
case when t1.id is null then t2.firstModifiedDiff else t1.firstModifiedDiff end,
case when t1.id is null then t2.t_from else t1.t_from end,
case when t1.id is null then t2.gps else t1.gps end,
case when t1.id is null then t2.offline else t1.offline end,
case when t1.id is null then t2.originalPrice else t1.originalPrice end,
case when t1.id is null then t2.price else t1.price end,
case when t1.id is null then t2.userNick else t1.userNick end,
case when t1.id is null then t2.categoryId else t1.categoryId end,
case when t1.id is null then t2.categoryName else t1.categoryName end,
case when t1.id is null then t2.fishPoolId else t1.fishPoolId end,
case when t1.id is null then t2.fishpoolName else t1.fishpoolName end,
case when t1.id is null then t2.bar else t1.bar end,
case when t1.id is null then t2.barInfo else t1.barInfo end,
case when t1.id is null then t2.abbr else t1.abbr end,
case when t1.id is null then t2.zhima else t1.zhima end,
case when t1.id is null then t2.shiren else t1.shiren end,
case when t1.id is null then t2.ts else t1.ts end
from
(select * from  wlbase_dev.t_base_ec_xianyu_iteminfo where ds = 'tmp')t1
full outer JOIN
(select * from wlbase_dev.t_base_ec_xianyu_iteminfo where ds = '20161218')t2
ON
t1.id = t2.id;
EOF

