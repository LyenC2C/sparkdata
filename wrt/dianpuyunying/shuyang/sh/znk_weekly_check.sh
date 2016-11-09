#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/dianpuyunying/shuyang'
save_path='/mnt/raid1/wrt/dianpuyunying/shuyang/development'
now_day=$1
last_day=$2
sold_day=$3

a=hive -e -S "use wlservice; select count(1) from t_wrt_znk_iteminfo_new where ds = 20161023"
echo a


#hive -e "use wlservice; select count(1) from t_wrt_znk_iteminfo_new where ds = 20161023"\
# >> $save_path/iteminfo_nowday_count
#
#hive<<EOF
#
#select count(1) from t_wrt_znk_iteminfo_new where ds = '$now_day'

#EOF

