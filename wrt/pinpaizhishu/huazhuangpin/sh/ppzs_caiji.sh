#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/pinpaizhishu/huazhuangpin'
save_path='/mnt/raid1/wrt/pinpaizhishu/development'
#每周六入库，周六当天计划任务执行该脚本即可
saturday=$(date -d '0 days ago' +%Y%m%d)
iteminfo_day="20161104"
hive -hiveconf saturday=$saturday iteminfo_day=$iteminfo_day -f $dev_path/t_wrt_caiji_ppzs_itemid.sql
