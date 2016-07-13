


/home/zlj/hive/bin/hive<<EOF


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_userbuy_info;
CREATE TABLE t_zlj_ec_perfer_tag_data
  AS

SELECT user_id,cate_level2_id,price
from
(
select user_id,cate_level2_id,price ,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY price  DESC) AS rn
from
(
	select user_id,cate_level2_id,sum(price) as   price
	from t_base_ec_dim  as t1 join  t_zlj_ec_userbuy as t2 on t2.cat_id=t1.cate_id where cate_level2_id is not null
 group by user_id ,cate_level2_id

)t

)t1
where rn< 15


EOF

spark-submit  --total-executor-cores  100   --executor-memory  15g  --driver-memory 15g   ../spark/user_cat_tag_v2.py