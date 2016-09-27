#!/usr/bin/env bash
source ~/.bashrc
zuotian=$(date -d '1 days ago' +%Y%m%d)
#last_week=$(date -d '7 days ago' +%Y%m%d)

#sh znk_upgrad.sh $zuotian 20160919 $zuotian &> znk_log/$zuotian_log
sh znk_upgrad.sh 20160925 20160919 20160919 &> znk_log/log_$zuotian