select * from t_zlj_user_tag_join_t where length(cat_tags)>20 and  cat_tags   like "%�з�%" and  user_profile like "%�Ĵ�%"   limit  1000;




select count(1) from t_zlj_user_tag_join_t where length(cat_tags)>20 and  cat_tags   like "%�з�%"  ;

-- 2702232
CREATE TABLE t_zlj_credit_house_feed_data AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE content LIKE  '装修';


-- 2199229






select * from t_zlj_credit_house_feed_data where rand()*100<0.5  limit 10000 ;



SELECT *
FROM t_base_ec_item_feed_dev
WHERE ds>20160101 and  content LIKE '装修' limit 100;
