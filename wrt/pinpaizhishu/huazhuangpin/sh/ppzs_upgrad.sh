#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/pinpaizhishu/huazhuangpin'
save_path='/mnt/raid1/wrt/pinpaizhishu/development'
#昨日时间（周日）
yes_day=$1
#上周日时间
last_day=$2
#三个月前的时间
threemonth_day=$3
#存储时间（一般为今日，周一）
save_day=$4

hfs -rmr /user/wrt/temp/ppzs_itemid_info

spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 \
$dev_path/ppzs_item_info.py $yes_day

hive<<EOF
LOAD DATA  INPATH '/user/wrt/temp/ppzs_itemid_info' OVERWRITE INTO TABLE
wlservice.ppzs_itemid_info PARTITION (ds=$yes_day);
EOF

#创建品牌周销量表：ppzs_brandid_weeksold
hive -hiveconf yes_day=$yes_day -hiveconf last_day=$last_day -f $dev_path/ppzs_brandid_weeksold.sql
#创建品牌评论表记录表与品牌好中差评表：ppzs_brandid_feed 与 ppzs_brandid_rate_count
hive -hiveconf yes_day=$yes_day -hiveconf threemonth_day=$threemonth_day -f $dev_path/ppzs_brandid_feed.sql
#产出品牌销量与中好差评到本地表（产出表均无分区）
hive -hiveconf yes_day=$yes_day -f $dev_path/ppzs_brandid_weeksold_feedcount.sql
#产出每个品牌的top6的商品信息表（产出表均无分区）
hive -hiveconf yes_day=$yes_day -f $dev_path/ppzs_brandid_top6item.sql
#产出每个品牌的随机6条好评和差评（产出表均无分区）
hive -hiveconf yes_day=$yes_day -f $dev_path/ppzs_brandid_6comment.sql

#hdfs到本地
hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_weeksold_feedcount/* \
> $save_path/ppzs_brandid_weeksold_feedcount_$save_day
hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_top6item/* \
> $save_path/ppzs_brandid_top6item_$save_day
hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_6comment/* \
> $save_path/ppzs_brandid_6comment_$save_day
#将三个文件传到开发机器上
scp $save_path/ppzs_brandid_weeksold_feedcount_$save_day gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/$save_day/
scp $save_path/ppzs_brandid_top6item_$save_day gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/$save_day/
scp $save_path/ppzs_brandid_6comment_$save_day gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/$save_day/



#hive -hiveconf yes_day='20161120' last_day='20161113' -f ppzs_brandid_weeksold.sql
#hive -hiveconf yes_day='20161120' 3month_day='201060820' -f ppzs_brandid_feed.sql
#hive -hiveconf yes_day='20161120' -f ppzs_weeksold_feedcount.sql
#hive -hiveconf yes_day='20161120' -f ppzs_brandid_top6item.sql
#hive -hiveconf yes_day='20161120' -f ppzs_brandid_6comment.sql
#hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_weeksold_feedcount/* > ./ppzs_brandid_weeksold_feedcount_20161121
#hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_top6item/* > ./ppzs_brandid_top6item_20161121
#hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_6comment/* > ./ppzs_brandid_6comment_20161121
#scp ./ppzs_brandid_weeksold_feedcount_20161121 gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/20161121/
#scp ./ppzs_brandid_top6item_20161121 gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/20161121/
#scp ./ppzs_brandid_6comment_20161121 gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/20161121/
#'${hiveconf:yes_day}'


#hive<<EOF
#LOAD DATA  INPATH '/user/wrt/temp/ppzs_itemid_info' OVERWRITE INTO TABLE
#wlservice.ppzs_itemid_info PARTITION (ds='20161120');
#EOF

