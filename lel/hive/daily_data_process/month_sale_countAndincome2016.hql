create table if not exists wlservice.t_lel_ec_monthsale_brand_2016
as
select t1.date as date ,t2.brand_name as brand_name,sum(t1.day_sold) as sale_count,sum(t1.day_sold_price) as income
from
(select item_id,day_sold,day_sold_price,substring(ds,0,6) as date
from
wlbase_dev.t_base_ec_item_daysale_dev_new
where ds regexp '2016'
) t1
join
(select item_id,brand_name
from
wl_base.t_base_ec_item_dev_new
where ds = 20170103
and brand_name  ("Nike/耐克","UNDER ARMOUR","Apple/苹果","Adidas/阿迪达斯")
) t2
on t1.item_id = t2.item_id
group by date,brand_name




create table if not exists wlservice.t_lel_ec_monthsale_cate_2016
as
select t1.date as date ,t2.root_cat_name as cat_name,sum(t1.day_sold) as sale_count,sum(t1.day_sold_price) as income
from
(select item_id,day_sold,day_sold_price,substring(ds,0,6) as date
from
wlbase_dev.t_base_ec_item_daysale_dev_new
where ds regexp '2016'
) t1
join
(select item_id,root_cat_name
from
wl_base.t_base_ec_item_dev_new
where ds = 20170103
and root_cat_name in ("手机","3C数码配件")
) t2
on t1.item_id = t2.item_id
group by date,brand_name