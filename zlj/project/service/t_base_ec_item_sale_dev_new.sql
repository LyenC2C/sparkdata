SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;



INSERT INTO TABLE t_base_ec_item_sale_dev_new PARTITION (ds )
select
item_id	 ,
r_price  ,
s_price   ,
quantity ,
total_sold  ,
order_cost  ,
ts	,
ds
from
t_base_ec_item_sale_dev  ;
