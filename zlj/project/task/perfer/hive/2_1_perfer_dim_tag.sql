

/home/hadoop/hive/bin/hive<<EOF


set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;

set hive.groupby.skewindata=true ;


drop table IF EXISTS  t_zlj_ec_perfer_dim;

create table t_zlj_ec_perfer_dim as


(
select  user_id, root_cat_id ,root_cat_name, f ,

row_number()  OVER (PARTITION BY user_id ORDER BY f desc) as rn

from
(
select
 user_id, root_cat_id ,root_cat_name ,sum(score) as f
from

t_zlj_ec_userbuy

group by user_id,root_cat_id,root_cat_name
) t

)t2
 ;

EOF


drop table IF EXISTS  t_zlj_ec_perfer_dim;

create table t_zlj_ec_perfer_dim as

select  user_id, root_cat_id, f ,

row_number()  OVER (PARTITION BY user_id ORDER BY f desc) as rn

from
(
select
 user_id, root_cat_id  ,sum(score) as f
from

t_zlj_ec_userbuy

group by user_id,root_cat_id
) t
 ;


SET hive.exec.reducers.bytes.per.reducer=500000000;
USE wlbase_dev;

DROP TABLE IF EXISTS t_zlj_ec_userbuy_info;
CREATE TABLE t_zlj_ec_perfer_tag_data
AS

 SELECT
  user_id,
  cate_level2_id,
  price
 FROM
  (
   SELECT
    user_id,
    cate_level2_id,
    price,
    ROW_NUMBER()
    OVER (PARTITION BY user_id
     ORDER BY price DESC) AS rn
   FROM
    (
     SELECT
      user_id,
      cate_level2_id,
      sum(price) AS price
     FROM t_base_ec_dim AS t1 JOIN t_zlj_ec_userbuy AS t2 ON t2.cat_id = t1.cate_id
     WHERE cate_level2_id IS NOT NULL
     GROUP BY user_id, cate_level2_id

    ) t

  ) t1
 WHERE rn < 15;