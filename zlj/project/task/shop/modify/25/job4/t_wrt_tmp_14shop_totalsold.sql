

-- 创建14个店铺的指定时间的商品日销量表
create table t_wrt_tmp_14shop_totalsold as
select
/*+ mapjoin(t2)*/
t2.shop_id as shop_id,t1.item_id as item_id,t2.title as title,t1.day_sold as sold,t1.day_sold_price as sales,t1.ds as ds from
(
select item_id,day_sold,day_sold_price,ds from wlbase_dev.t_base_ec_item_daysale_dev_new where ds > 20160400 and ds <20160600
)t1
join
(
select item_id,shop_id,price,title,ds from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615
)t2
ON
t1.item_id = t2.item_id;


