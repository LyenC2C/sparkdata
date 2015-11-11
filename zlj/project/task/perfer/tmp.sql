
--  ses
select
  (datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),'2015-08-21')-40)/4.0,
  power(0.5,6.5)
  from wlbase_dev.t_base_ec_item_feed_dev
    where ds=20150810
 limit 10




select
  count(1)
from t_zlj_ec_perfer_priceavg
where avg_price<1


select count(1),bc_type from t_base_ec_item_dev where  ds=20151026 group by  bc_type
select *  from t_base_ec_item_dev where ds=20151026 group by  bc_type='4.0'


