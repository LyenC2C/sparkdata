select * from t_zlj_user_tag_join_t where length(cat_tags)>20 and  cat_tags   like "%有房%" and  user_profile like "%四川%"   limit  1000;




select count(1) from t_zlj_user_tag_join_t where length(cat_tags)>20 and  cat_tags   like "%有房%"  ;

-- 2702232
CREATE TABLE t_zlj_credit_house_feed_data AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE content LIKE '%装修%';


-- 2199229
CREATE TABLE t_zlj_credit_house_feed_data_item AS
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
FROM t_zlj_credit_house_feed_data t1 JOIN t_base_ec_item_dev_new   t2
ON t1.item_id = t2.item_id AND t2.ds = 20160530;




select * from t_zlj_credit_house_feed_data where rand()*100<0.5  limit 10000