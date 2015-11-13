/home/hadoop/hive/bin/hive<<EOF


SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions = 2000;

SET hive.exec.reducers.bytes.per.reducer = 500000000;

USE wlbase_dev;


CREATE TABLE t_zlj_userbuy_freq AS
SELECT
  user_id,
  week,
  t_month,
  count(1) AS times
FROM
  (
    SELECT
      item_id,
      user_id,
      month(f_date)      AS t_month,
      weekofyear(f_date) AS week
    FROM t_base_ec_item_feed_dev
    WHERE ds > 20150101
  ) t
GROUP BY user_id, week, t_month;

EOF
