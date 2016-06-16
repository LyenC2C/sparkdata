SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.created.files=655350;
set hive.exec.reducers.bytes.per.reducer=500000000;



INSERT INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds)
select
*
from
t_base_ec_item_daysale_dev where day_sold>0 and ds > 20160315;



create table t_base_ec_item_daysale_dev_v2 like t_base_ec_item_daysale_dev_new;

INSERT overwrite TABLE t_base_ec_item_daysale_dev_v2 PARTITION (ds)
select t1.item_id,t1.day_sold,
case when (t1.day_sold_price/t1.day_sold) - t2.price > 1000000 then t2.price * t1.day_sold
else t1.day_sold_price end,t1.ds as ds
from
(select * from t_base_ec_item_daysale_dev_new )t1

join

(select item_id,price from t_base_ec_item_dev_new where ds = 20160615)t2

ON
t1.item_id = t2.item_id;



