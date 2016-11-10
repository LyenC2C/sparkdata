#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/dianpuyunying/shuyang'
zuotian=$(date -d '1 days ago' +%Y%m%d)
last_week=$(date -d '8 days ago' +%Y%m%d)
jintian=$(date -d '0 days ago' +%Y%m%d)

#sh znk_upgrad.sh $zuotian 20160919 $zuotian &> znk_log/$zuotian_log

sh $dev_path/sh/znk_upgrad.sh $zuotian $last_week $jintian &> $dev_path/sh/znk_log/log_$jintian