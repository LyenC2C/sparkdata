#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/pinpaizhishu/huazhuangpin'
save_path='/mnt/raid1/wrt/pinpaizhishu/development'
#每周六入库，周六当天计划任务执行该脚本即可,参数为周五，因为需要取周五的shopitem数据
friday=$(date -d '1 days ago' +%Y%m%d)
iteminfo_day="20161104"
hive -hiveconf friday=$friday iteminfo_day=$iteminfo_day -f $dev_path/t_wrt_caiji_ppzs_itemid.sql
