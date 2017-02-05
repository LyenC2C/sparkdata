#!/usr/bin

source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
last_2_days=$(date -d '2 days ago' +%Y%m%d)


table=wlbase_dev.t_base_ec_xianyu_iteminfo

hive<<EOF
use wlbase_dev;
select * from $table where ds = $last_2_days limit 10;
EOF