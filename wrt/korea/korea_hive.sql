
select t1.aid,t1.amount from
(select aid,amount from t_wrt_korea_itemsale )t1
JOIN
(select item_id,cate_root_name,price from t_wrt_korea_iteminfo where cate_root_name = '服饰鞋包')t2
ON
t1.aid = t2.item_id
order by cast(t1.amount as int ) DESC



insert overwrite table t_wrt_tmallint_itemid
select item_id from t_tc_korea_country;



select aid,amount from t_tc_korea where cate_root_name = '服饰鞋包' order by cast(t1.amount as int ) DESC;


create table t_wrt_korea_sales_11_03(
aid String,
brand_id String,
brand_name String,
country String,
day_sold String,
day_sold_price String,
ds String
);


insert overwrite table t_wrt_korea_sales_11_03
select t1.aid,t1.brandid,t1.brand_name,t1.country,t2.day_sold,t2.day_sold_price,t2.ds from
(select aid,brandid,brand_name,country from t_tc_korea_country)t1
JOIN
(select item_id,day_sold,day_sold_price,ds from wlbase_dev.t_base_ec_item_daysale_dev where ds >= 20151101 and ds <= 20160229)t2
ON
t1.aid = t2.item_id;




select   brand_id, brand_namem ,m ,sales
 from
(
select brand_id, brand_namem ,m ,sales ,

   ROW_NUMBER() OVER (PARTITION BY brand_id,  m   ORDER BY sales DESC) AS rn
from
(
select
brand_id, max(brand_name) brand_name,sum(day_sold_price) as sales ,m

 from
   (
   select brand_id, brand_name,SUBSTR(ds,6,7) as m,day_sold_price  from t_wrt_korea_sales_11_03
   )

   t1
   group by brand_id,m
   )
t

)t2 where rn<100;

select brand_id,name,month,sales from
(select brand_id,max(brand_name), as name,SUBSTR(ds,3,2) as month,sum(day_sold_price) as sales group by brand_id,month)t
order by sales limit 100;

select sum(day_sold_price),SUBSTR(ds,5,2) as month from t_wrt_korea_sales_11_03 group by month;

select sum(day_sold_price),SUBSTR(ds,5,2)  from t_wrt_korea_sales_11_03 where country = '韩国' group by SUBSTR(ds,5,2)

select sum(day_sold),SUBSTR(ds,5,2)  from t_wrt_korea_sales_11_03 where country = '韩国' group by SUBSTR(ds,5,2)

select brand_id,name,coun,sales,sold from
(
select brand_id,max(brand_name) as name,max(country) as coun ,sum(day_sold_price)as sales,sum(day_sold) as sold
from t_wrt_korea_sales_11_03 where
SUBSTR(ds,5,2) = 02
group by brand_id
)t
order by sales DESC limit 100 ;

