



CREATE TABLE t_zlj_shop_join_major AS
  SELECT
    t1.*,
    CASE WHEN main_cat_name IS NULL
      THEN '-'
    ELSE main_cat_name END AS main_cat_name
  FROM
    (
      SELECT *
      FROM
        t_base_ec_shop_dev_new
      WHERE ds = 20160615 AND desc_highgap < 100 AND service_highgap < 100 AND wuliu_highgap < 100
    ) t1 LEFT JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id ;




SELECT  * FROM

(
SELECT  main_cat_name,shop_id ,shop_name ,count(1) as total_rn,
  ROW_NUMBER()
        OVER (PARTITION BY main_cat_name
          ORDER BY desc_score DESC ) AS rn
from
t_zlj_shop_join_major  where main_cat_name in ('家居用品','美容护理')

)t where shop_id in ('104820621','103569798')
;


SELECT count(1)  ,main_cat_name
from
t_zlj_shop_join_major  where main_cat_name in ('家居用品','美容护理') ;

SELECT bc_type,max(cast(credit as bigint ) )      FROM         t_base_ec_shop_dev_new       WHERE ds = 20160613  group by bc_type;


   SELECT  * from  t_base_ec_shop_dev_new       WHERE ds = 20160615  and credit=20  limit 100;
-- 店铺信用等级增长


SELECT cat_name,count(1)
from t_base_ec_item_dev_new where ds=20160615 and shop_id='57299948' group by cat_name ;

SELECT
  substring(ds,0,6) as m, avg(credit) as a_credit ,avg(fans_count)  ,shop_id ,shop_name
  FROM
    t_base_ec_shop_dev where shop_id in ('104820621','103569798','57299948')
group by substring(ds,0,6) ,shop_id ,shop_name;





-- 用户信用等级分析
create table t_zlj_shop_shop_user_level_verify as
SELECT t1.* ,t2.shop_id  from
(
SELECT alipay, verify,tb_id
  FROM  t_base_user_info_s_tbuserinfo_t where alipay  is not null and length(verify)>2
)t1
join
(
select shop_id,user_id
from  t_base_ec_record_dev_new
where shop_id in ('104820621','103569798','57299948')
)t2 on t1.tb_id=t2.user_id ;


SELECT  shop_id, count(1),verify
  FROM  t_zlj_shop_shop_user_level_verify group by shop_id ,verify

;


create table t_zlj_shop_shop_57299948_day_sold as
SELECT  /*+ mapjoin(t2)*/
  day_sold,day_sold_price ,ds

from (SELECT  * from  t_base_ec_item_daysale_dev_new   ) t1  join
  (SELECT
  item_id ,shop_id
  FROM
    t_base_ec_item_dev_new where ds=20160615  and shop_id='57299948'
    )t2  on  t1.item_id =t2.item_id ;


SELECT  max(day_sold_price)  from t_zlj_shop_shop_57299948_day_sold

SELECT
  sum(day_sold),sum(day_sold* case when day_sold_price>500 then 500 else  day_sold_price end  ),substring(ds,0,6)
  FROM
     t_zlj_shop_shop_day_sold
group by substring(ds,0,6) ;






SELECT  /*+ mapjoin(t2)*/
sum(day_sold),sum(day_sold_price) ,substring(ds,0,6)

from (SELECT  * from  t_base_ec_item_daysale_dev_new  ) t1  join
  (SELECT
  item_id ,shop_id
  FROM
    t_base_ec_item_dev_new where ds=20160615  and shop_id='57299948'
    )t2  on  t1.item_id =t2.item_id
group by substring(ds,0,6) ;