--此表主要用于采集组采集使用

use wlservice;
create table t_wrt_znk_shopid
(
shopid string,
bc_type string
)
COMMENT '纸尿裤shopid'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n';

insert into table t_wrt_znk_shopid partition(ds = 20160901)
select shop_id,max(bc_type) from t_wrt_znk_iteminfo group by shop_id