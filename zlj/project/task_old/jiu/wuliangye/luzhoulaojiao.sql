CREATE TABLE t_base_ec_record_dev_wine_0407
  AS
    SELECT
      item_id,
      title,
      feed_id,
      user_id,
      content_length,
      annoy,
      ds,
      datediff,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      bc_type,
      price,
      location
    FROM t_base_ec_record_dev_new
    WHERE cat_id in ( 50008144,50013052) and ds>20151230  and ds<20160401;


-- 09

select
m,sum(CASE WHEN tage >= 18 AND tage <= 24
    THEN 1 END),
  sum(CASE WHEN tage >= 25 AND tage <= 29
    THEN 1 END),
  sum(CASE WHEN tage >= 30 AND tage <= 34
    THEN 1 END),
  sum(CASE WHEN tage >= 35 AND tage <= 39
    THEN 1 END),
  sum(CASE WHEN tage >= 40 AND tage <= 49
    THEN 1 END),
  sum(CASE WHEN tage >= 50 AND tage <= 59
    THEN 1 END)

from
(
select

t1.tb_id,t1.tgender,tage ,split(t1.tloc,'\\s+')[0] as tloc ,m
from
t_base_user_info_s t1

join
(
select *, substr(ds,5,2) as m from  t_base_ec_record_dev_wine_0407
where brand_id=4537002
)t2
 on t1.tb_id=t2.user_id  and t1.ds=20160310 and LENGTH(t1.tloc)>0

 )t where tage>0 and tage<100  and m<5  group by m  ;



 select m,tloc,count(1)

 from
 (
select

t1.tb_id,t1.tgender,tage ,split(t1.tloc,'\\s+')[0] as tloc ,m
from
t_base_user_info_s t1

join
(
select *, substr(ds,5,2) as m from  t_base_ec_record_dev_wine_0407
where brand_id=4537002
)t2
 on t1.tb_id=t2.user_id  and t1.ds=20160310 and LENGTH(t1.tloc)>0

 )t where tage>0 and tage<100  and m<5  group by m,tloc  ;



select *
from
(
select  brand_id,m ,sum(1),sum(1)*max(price) sale
 from
(
 select
 user_id
 from t_base_ec_record_dev_wine_0407
 where item_id=522579730902
 GROUP  by user_id
 )t1 join
(
select *, substr(ds,5,2) as m from  t_base_ec_record_dev_wine_0407
where brand_id<>4537002
)t2  on t1.user_id=t2.user_id
group by brand_id,m

)t3 order by sale desc limit 100;


