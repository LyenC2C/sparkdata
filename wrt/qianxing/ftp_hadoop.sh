#!/usr/bin/env bash
jintian=$(date -d '0 days ago' +%Y%m%d)

#wget ftp://ftp.blhdk.wolongdata.com/$zuotian*.csv --ftp-user=qx --ftp-password=NzS7d2oWe47LPVXx --no-passive-ftp

today=`ls $jintian*`
python jiema.py $today > $jintian'_qianxing'

hadoop fs -put ./$jintian_qianxing /user/wrt/qianxing/



#hive<<EOF
#load data inpath '/user/wrt/qianxing/*' insert overwrite table wl_link.t_qianxing_huafei partition(ds =$zuotian)
#EOF


