USE wlbase_dev;

-- drop table if EXISTS  t_zlj_ec_maxfeedId;

-- CREATE  table  t_zlj_ec_maxfeedId as

SET mapred.max.split.size = 256000000;
SET mapred.min.split.size.per.node = 256000000;
SET mapred.min.split.size.per.rack = 256000000;
SET hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;


INSERT OVERWRITE TABLE t_base_ec_feed_add_everyday PARTITION(ds=20151101)
SELECT
  item_id,
  max(CAST(feed_id AS BIGINT)) feedid,
  count(1) AS                  feed_times

FROM
  t_base_ec_item_feed_dev
WHERE LENGTH(item_id) > 5
GROUP BY item_id;

-- -- SELECT
-- --  item_id,feed_id,f_date,rn
-- -- FROM
-- -- (
-- SELECT
-- item_id,feed_id,f_date,
-- -- row_number()  OVER (PARTITION BY item_id ORDER BY CAST (feed_id as bigint) desc) as rn
-- FROM
-- t_base_ec_item_feed_dev
-- group by item_id
-- -- WHERE ds
-- -- )t where rn=1



INSERT OVERWRITE TABLE t_base_ec_item_sale_dev PARTITION(ds=20151119)
SELECT
  item_id,
  item_title,
  r_price,
  s_price,
  bc_type,
  quantity,
  total_sold,
  order_cost,
  shop_id,
  ts
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY ts DESC) AS rn
    FROM t_base_ec_item_sale_dev
    where ds=20151115
  )y
WHERE rn = 1