CREATE TABLE t_zlj_credit_girl_feed_data AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE   ds>20160101 and content LIKE '%女朋友%'  or  content LIKE '%妹子%';



CREATE TABLE t_zlj_credit_girl_feed_data_girllove AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE    content LIKE '%女朋友%'



CREATE TABLE t_zlj_credit_girl_feed_data_girllove_item AS
SELECT
 t1.*,
 t2.cat_id,
 cat_name,
 root_cat_id,
 root_cat_name,
 price,
 bc_type,
 brand_name,
 title
FROM t_zlj_credit_girl_feed_data_girllove t1 JOIN t_base_ec_item_dev_new   t2
ON t1.item_id = t2.item_id AND t2.ds = 2016050;



select cat_name,count(1)  from t_zlj_credit_girl_feed_data_girllove_item where
 root_cat_name like '%鲜花%'group by cat_name

	290501	鲜花速递(同城)

create table  t_zlj_credit_girl_feed_data_girllove_item_flower_290501 as
select * from  t_base_ec_record_dev_new where cat_id=290501 ;

-- 直接根据年龄来吧