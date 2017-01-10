#!/bin/bash
source ~/.bashrc
date
date  +%Y%m%d


lastday=$(date -d '1 days ago' +%Y%m%d)
last_7_day=$(date -d '7 days ago' +%Y%m%d)
hadoop fs -test -e /user/lel/temp/xianyu_userinfo
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/xianyu_userinfo
else
    echo 'Directory is not exist'
fi

spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 60 ~/wolong/sparkdata/lel/spark/xianyu/t_xianyu_userinfo.py $lastday

hive<<EOF

use wlbase_dev;
LOAD DATA  INPATH '/user/lel/temp/xianyu_userinfo' OVERWRITE INTO TABLE wlbase_dev.t_base_ec_xianyu_userinfo PARTITION (ds='tmp');

insert into table t_base_ec_xianyu_userinfo partition(ds = $lastday+'userid_makeup')
select
case when t2.tb_nick is null then t1.userid else t2.tb_id end,
t1.totalcount,
t1.gender,
t1.idleuserid,
t1.nick,
t1.tradecount,
t1.tradeincome,
t1.usernick,
t1.constellation,
t1.birthday,
t1.city,
t1.infopercent,
t1.signature,
t1.ts
from
(select * from t_base_ec_xianyu_userinfo where ds = 'tmp') t1
left join
(select tb_id,tb_nick from t_base_user_profile) t2
on t1.usernick = t2.tb_nick;

insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_userinfo PARTITION(ds = $lastday)
select
case when t1.userid is null then t2.userid else t1.userid end,
case when t1.userid is null then t2.totalcount else t1.totalcount end,
case when t1.userid is null then t2.gender else t1.gender end,
case when t1.userid is null then t2.idleuserid else t1.idleuserid end,
case when t1.userid is null then t2.nick else t1.nick end,
case when t1.userid is null then t2.tradecount else t1.tradecount end,
case when t1.userid is null then t2.tradeincome else t1.tradeincome end,
case when t1.userid is null then t2.usernick else t1.usernick end,
case when t1.userid is null then t2.constellation else t1.constellation end,
case when t1.userid is null then t2.birthday else t1.birthday end,
case when t1.userid is null then t2.city else t1.city end,
case when t1.userid is null then t2.infopercent else t1.infopercent end,
case when t1.userid is null then t2.signature else t1.signature end,
case when t1.userid is null then t2.ts else t1.ts end
from
(select * from  wlbase_dev.t_base_ec_xianyu_userinfo where ds = $lastday+'userid_makeup') t1
full outer JOIN
(select * from wlbase_dev.t_base_ec_xianyu_userinfo where ds = $last_7_day) t2
ON
t1.userid = t2.userid;
EOF

