create table t_wrt_branddaysale_datatao(
brand_id String,
total_sold String,
day_sold STRING,
day_sold_price STRING,
ds String
)

insert overwrite table t_wrt_brandsale_datatao
select /*+ mapjoin(t1)*/
t1.brand_id,sum(t1.total_sold) as total_sold,sum(t2.day_sold)as day_sold,sum(t2.day_sold_price) as day_sold_price,t1.ds from
(
select /*+ mapjoin(t_wrt_brandcount_datatao)*/
tt.brand_id,tt.item_id,tt.total_sold,tt.ds FROM
t_wrt_brandcount_datatao
JOIN
(
select brand_id,item_id,total_sold,ds from
t_base_ec_item_sale_dev where ds >= 20160301 and ds<= 20160310
)tt
ON
tt.brand_id = t_wrt_brandcount_datatao.brand_id
)t1
join
(
select item_id,day_sold,day_sold_price,ds from
t_base_ec_item_daysale_dev where ds >= 20160301 and ds<= 20160310
)t2
ON
t1.item_id = t2.item_id and t1.ds = t2.ds
group by t1.brand_id,t1.ds;