
select cate_name,count(1) as sale ,count(DISTINCT brandid) as brand_count
from  t_tc_korea where  cate_root_name="ÃÀ×±¸ö»¤"
group by cate_name ;


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