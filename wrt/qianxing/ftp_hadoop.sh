#!/usr/bin/env bash
jintian=$(date -d '0 days ago' +%Y%m%d)
pre_path='/home/wrt/data/qianxing'

wget ftp://ftp.blhdk.wolongdata.com/$jintian*.csv --ftp-user=qx --ftp-password=NzS7d2oWe47LPVXx \
--no-passive-ftp -P $pre_path/

today=`ls $jintian*`
python jiema.py $pre_path/$today > $pre_path/$jintian'_qianxing'

hadoop fs -put $pre_path/$jintian'_qianxing' /user/wrt/qianxing/

hadoop fs -chmod -R 777 /user/wrt/qianxing/*


beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
load data inpath '/user/wrt/qianxing/$jintian*' overwrite into table wl_link.t_base_qianxing_order partition(ds =$jintian)
EOF
