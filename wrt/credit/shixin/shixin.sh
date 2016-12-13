#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata/wrt/credit/shixin'
now_day = $(date -d '0 days ago' +%Y%m%d)
#last_day = $(date -d '7 days ago' +%Y%m%d)
last_day = "20161205"

sh ./shixin_person_inhive.sh $now_day $last_day
sh ./shixin_qiye_inhive.sh $now_day $last_day
