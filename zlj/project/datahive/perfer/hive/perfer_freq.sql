

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;

use wlbase_dev;


create table t_zlj_userbuy_freq  as
SELECT
  user_id,
  week,
  t_month ,
  count(1) AS times
FROM
  (
    SELECT
      item_id,
      user_id,
      month(f_date) as t_month,
      weekofyear(f_date) AS week
    FROM t_base_ec_item_feed_dev
    WHERE ds > 20150101
  ) t
group by user_id ,week, t_month;

SELECT times,count(1)
FROM(
  SELECT cast(times as string )
FROM t_zlj_userbuy_freq
 )t GROUP BY  times;