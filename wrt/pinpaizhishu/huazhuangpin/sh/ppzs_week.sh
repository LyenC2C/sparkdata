#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/pinpaizhishu/huazhuangpin'
yes_day=$(date -d '1 days ago' +%Y%m%d)
last_day=$(date -d '8 days ago' +%Y%m%d)
threemonth_day=$(date -d '93 days ago' +%Y%m%d)
save_day=$(date -d '0 days ago' +%Y%m%d)

sh $dev_path/sh/ppzs_upgrad.sh 20161120 20161114 20160820 20161121
#sh $dev_path/sh/ppzs_upgrad.sh $yes_day $last_day $threemonth_day $save_day


