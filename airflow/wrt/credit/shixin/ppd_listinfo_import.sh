#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/ppd/'
now_day=$(date -d '0 days ago' +%Y%m%d)

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
LOAD DATA INPATH '/user/wrt/temp/ppd_info_tmp' OVERWRITE INTO TABLE wl_base.t_base_credit_ppd_listinfo PARTITION (ds=$now_day);
EOF
