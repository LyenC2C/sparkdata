
-- 根据评价预估销量

select
item_id,sum(day_sold) sa solds
from
t_base_ec_item_daysale_dev_new group by item_id

select
COUNT (1)
from
(
select  item_id
from t_base_ec_item_daysale_dev_new
group by item_id
)t;



create table t_zlj_korea_feed_count as
select  item_id ,count(1) as num
from t_base_ec_item_feed_dev where ds>20140601 group by item_id;





create table t_zlj_korea_feed_count_brand_cat as
select  item_id,cat_id,root_cat_id,brand_id ,count(1) as num

from t_base_ec_record_dev_new  group by item_id,cat_id,root_cat_id,brand_id ;


create table t_zlj_korea_sold_itemid_count_new as
select

t1.item_id,cat_id,root_cat_id,brand_id,brand_name,total_sold,s_price
from
(
 select item_id,total_sold,s_price from t_base_ec_item_sale_dev_new where ds=20160316
  )t1 join t_base_ec_item_dev t2 on t2.ds=20160333 and t1.item_id=t2.item_id  ;




create table t_zlj_korea_sold_itemid_count as
select  item_id,count(1) as num
from t_base_ec_item_daysale_dev_new
group by item_id;


create table t_zlj_korea_feed_sold_p as
select
t1.item_id,t1.root_cat_id , t1.brand_id,t2.brand_name  ,t1.num as feed_num,t2.total_sold as sold_num  ,t1.num/t2.total_sold as  p
from
t_zlj_korea_feed_count_brand_cat  t1
join t_zlj_korea_sold_itemid_count_new t2 on t1.item_id=t2.item_id;


select  root_cat_id , brand_id ,sum(feed_num)/sum(sold_num) from  t_zlj_korea_feed_sold_p group by  root_cat_id , brand_id ;


select brand_id, sum(sold_num)/COUNT(1)  as brand_avg from t_zlj_korea_feed_sold_p group by brand_id;

select root_cat_id, sum(sold_num)/COUNT(1)  as root_cat_avg from t_zlj_korea_feed_sold_p group by root_cat_id;



create table t_zlj_korea_feedsold_brand_count as

select brand_id, brand_name ,sum(sold_num)/COUNT(1) as savg,  (sum(feed_num)+1)/sum(sold_num) as favg  from t_zlj_korea_feed_sold_p group by brand_id,brand_name




create table t_zlj_korea_tmhk_sold_andpre as
SELECT total_sold,pre_sold, root_cat, brand, price, comment_count, key1, title, t1.item_id, category_final, site, rank
from
( select item_id,total_sold from t_base_ec_item_sale_dev_new where ds=20160316) t1
join (select pre_sold, root_cat, brand, price, comment_count, key1, title, item_id, category_final, site, rank  FROM t_base_ec_hanguo where site='tmhk') t2 on t1.item_id =t2.item_id;

-- select p_s ,count(1) as num from
-- (
-- select  p*100/100 as p_s
-- from t_zlj_korea_feed_sold_p )
-- t group by p_s
--
--
-- select p_s ,log(count(1))  as num from
-- (
-- select  cast(p*100 as int )%100 as p_s
-- from t_zlj_korea_feed_sold_p )
-- t group by p_s ;