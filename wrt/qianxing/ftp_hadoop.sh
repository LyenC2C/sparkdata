#!/usr/bin/env bash
jintian=$(date -d '0 days ago' +%Y%m%d)
zuotian=$(date -d '1 days ago' +%Y%m%d)
pre_path='/home/wrt/data/qianxing'

wget ftp://ftp.blhdk.wolongdata.com/$jintian*.csv --ftp-user=qx --ftp-password=NzS7d2oWe47LPVXx \
--no-passive-ftp -P $pre_path/

today=`ls $pre_path/$jintian*`
python jiema.py $today > $pre_path/$jintian'_qianxing'

hadoop fs -put $pre_path/$jintian'_qianxing' /user/wrt/qianxing/

hadoop fs -chmod -R 777 /user/wrt/qianxing/*

spark-submit --executor-memory 6G  --driver-memory 8G  --total-executor-cores 100 qianxing_iteminfo.py $jintian $zuotian

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
load data inpath '/user/wrt/qianxing/$jintian*' overwrite into table wl_link.t_base_qianxing_order partition(ds = 'temp');
insert overwrite table wl_link.t_base_qianxing_order partition (ds = $jintian)
select
t1.user_nick,
t1.user_alipay,
t1.price,
t1.user_realname,
t1.phone,
t1.create_time,
t1.pay_time,
t1.title,
t2.item_id
from
(select * from wl_link.t_base_qianxing_order where ds = 'temp')t1
left join
(select title,max(item_id) as item_id from  wl_base.t_base_qianxing_iteminfo where ds = $jintian group by title)t2
on
t1.title = t2.title
EOF


