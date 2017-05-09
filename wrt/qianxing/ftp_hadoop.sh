#!/usr/bin/env bash
jintian=$(date -d '0 days ago' +%Y%m%d)

wget ftp://ftp.blhdk.wolongdata.com/$zuotian*.csv --ftp-user=qx --ftp-password=NzS7d2oWe47LPVXx --no-passive-ftp

today=`ls $jintian*`
python jiema.py $today > $jintian'_qianxing'

hadoop fs -put ./$jintian'_qianxing' /user/wrt/qianxing/

hadoop fs -chmod -R 777 /user/wrt/qianxing/*


beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
load data inpath '/user/wrt/qianxing/$jintian*' overwrite into table wl_link.t_base_qianxing_order partition(ds =$jintian)
EOF
