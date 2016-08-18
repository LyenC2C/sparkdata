

-- 用户购买类目数据
Drop table  t_base_ec_record_dev_new_simple_user_rootid;

CREATE TABLE t_base_ec_record_dev_new_simple_user_rootid AS

  SELECT
    user_id,
    root_cat_id,
    count(1)   AS num ,
    avg(price) AS avg_price ,
    max(price) as max_price ,
    percentile(cast(price as BIGINT),0.5)  mid_price
  FROM t_base_ec_record_dev_new_simple
    where cast(user_id as BIGINT)>0 and  cast(root_cat_id as BIGINT)>0
  GROUP BY user_id, root_cat_id
  ;
