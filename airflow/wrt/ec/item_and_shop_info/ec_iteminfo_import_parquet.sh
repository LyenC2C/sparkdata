#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
drop table if exists wl_base.t_base_ec_item_dev_new_parquet;
create table wl_base.t_base_ec_item_dev_new_parquet STORED AS PARQUET
as
select * from wl_base.t_base_ec_item_dev_new where ds = $today;
EOF