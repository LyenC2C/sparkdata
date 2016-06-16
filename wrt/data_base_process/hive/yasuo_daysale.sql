SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;



INSERT INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds)
select
*
from
t_base_ec_item_daysale_dev where day_sold>0 and ds > 20160315;