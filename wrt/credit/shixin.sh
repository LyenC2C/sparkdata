#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata/wrt/credit'
now_day=$(date -d '0 days ago' +%Y%m%d)
#last_day = $(date -d '7 days ago' +%Y%m%d)
last_day="20161205"

sh $pre_path/shixin/shixin_person_inhive.sh $now_day $last_day &> $pre_path/log/shixin_person_$now_day
sh $pre_path/shixin/shixin_qiye_inhive.sh $now_day $last_day &> $pre_path/log/shixin_qiye_$now_day
