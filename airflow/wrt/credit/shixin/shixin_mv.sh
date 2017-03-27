#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata/wrt/credit'
now_day=$(date -d '0 days ago' +%Y%m%d)

hadoop fs -mkdir /commit/credit/shixin/archive/$now_day'_arc'
hadoop fs -mv /commit/credit/shixin/shixin* /commit/credit/shixin/archive/$now_day'_arc'/






