-- insert overwrite table ppzs_brandid_top6item partition (ds = '${hiveconf:yes_day}')
select brand_id,item_id,title,price,picurl,now_sold,rn
from
(
select
t1.item_id,
t1.brand_id,
t1.title,
t1.price,
t1.picurl,
t2.now_sold,
ROW_NUMBER() OVER (PARTITION BY t1.brand_id ORDER BY t2.now_sold DESC) as rn
from
(select brand_id,item_id,title,price,picurl from wlservice.ppzs_itemid_info where ds = '${hiveconf:yes_day}')t1
JOIN
(
select item_id,sold as now_sold from wlbase_dev.t_base_ec_shopitem_b where ds = '${hiveconf:yes_day}'
group by item_id
)t2
ON
t1.item_id = t2.item_id
)t
where rn < 7;