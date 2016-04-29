SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;



INSERT INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds )
select
*
from
t_base_ec_item_daysale_dev where day_sold>0 ;


--

create table t_base_ec_item_daysale_dev_iteminfo
as
select
t2.*,t1.ds as tds , t1.day_sold,day_sold_price,day_sold_price*day_sold as sale_all
from t_base_ec_item_daysale_dev_new t1
join t_base_ec_item_dev   t2 on  t2.ds=20160333 and t1.item_id=t2.item_id