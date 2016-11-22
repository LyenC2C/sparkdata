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

hive -hiveconf yes_day=$yes_day last_day=$last_day -f $dev_path/ppzs_brandid_weeksold.sql
hive -hiveconf yes_day=$yes_day threemonth_day=$threemonth_day -f $dev_path/ppzs_brandid_feed.sql
hive -hiveconf yes_day=$yes_day -f $dev_path/ppzs_brandid_weeksold_feedcount.sql

hfs -cat /hive/warehouse/wlservice.db/ppzs_brandid_weeksold_feedcount/* \
> $save_path/7week/ppzs_brandid_weeksold_feedcount_$save_day

scp $save_path/7week/ppzs_brandid_weeksold_feedcount_$save_day gutao@10.2.4.177:/home/gutao/data/brand_data/tongtong/7week_history/


