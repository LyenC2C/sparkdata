create table t_wrt_outdoor_sale as
select  t1.*,t2.sold as april_sold,t2.sales as april_sales,t3.sold as may_sold,t3.sales as may_sales from
(select
t_lzh_outdoorid_temp.item_id,
t_lzh_outdoorid_temp.title,
t_lzh_outdoorid_temp.root_cat_name,
t_lzh_outdoorid_temp.shop_name,
t_lzh_outdoorid_temp.shop_id,
t_lzh_outdoorid_temp.price,
t_lzh_outdoorid_temp.brand_id,
t_lzh_outdoorid_temp.brand_name,
t_lzh_outdoorid_temp.bc_type,
sum(t.day_sold) as sold, sum(t.day_sold_price) as sales from
t_lzh_outdoorid_temp
join
(select * from wlbase_dev.t_base_ec_item_daysale_dev where ds >20160300 and ds < 20160400)t
on
t_lzh_outdoorid_temp.item_id = t.item_id
group by
t_lzh_outdoorid_temp.item_id,
t_lzh_outdoorid_temp.title,
t_lzh_outdoorid_temp.root_cat_name,
t_lzh_outdoorid_temp.shop_name,
t_lzh_outdoorid_temp.shop_id,
t_lzh_outdoorid_temp.price,
t_lzh_outdoorid_temp.brand_id,
t_lzh_outdoorid_temp.brand_name,
t_lzh_outdoorid_temp.bc_type
)t1
join
(select t_lzh_outdoorid_temp.item_id,sum(t.day_sold) as sold, sum(t.day_sold_price) as sales from
t_lzh_outdoorid_temp
join
(select * from wlbase_dev.t_base_ec_item_daysale_dev where ds >20160400 and ds < 20160500)t
on
t_lzh_outdoorid_temp.item_id = t.item_id
group by t_lzh_outdoorid_temp.item_id)t2
on t1.item_id = t2.item_id
join
(select t_lzh_outdoorid_temp.item_id,sum(t.day_sold) as sold, sum(t.day_sold_price) as sales from
t_lzh_outdoorid_temp
join
(select * from wlbase_dev.t_base_ec_item_daysale_dev where ds >20160500 and ds < 20160600)t
on
t_lzh_outdoorid_temp.item_id = t.item_id
group by t_lzh_outdoorid_temp.item_id)t3
on
t1.item_id = t3.item_id;