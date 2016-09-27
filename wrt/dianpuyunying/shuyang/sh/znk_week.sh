#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/dianpuyunying/shuyang'
zuotian=$(date -d '1 days ago' +%Y%m%d)
#last_week=$(date -d '7 days ago' +%Y%m%d)

#sh znk_upgrad.sh $zuotian 20160919 $zuotian &> znk_log/$zuotian_log

sh $dev_path/sh/znk_upgrad.sh 20160925 20160919 20160924 &> $dev_path/sh/znk_log/log_$zuotian