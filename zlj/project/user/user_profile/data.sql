CREATE TABLE IF NOT EXISTS t_ec_zlj_userbuy_history (
  user_id STRING COMMENT 'id',
  item_id STRING,
  content_len INT,
  f_date STRING,
  annoy STRING,
  root_cat_id STRING,
  cate_level2_id STRING,
  cat_id STRING,
  brand_id STRING
)
  COMMENT '购买记录表'
  PARTITIONED BY  (ds STRING )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
  stored AS RCFile;


INSERT OVERWRITE TABLE t_ec_zlj_userbuy_history PARTITION(ds='20160328')
SELECT
  user_id,
  t2.item_id,
  content_len,
  f_date,
  annoy,
  root_cat_id,
  cate_level2_id,
  cat_id,
  brand_id

FROM
  (
    SELECT
      /*+ mapjoin(t3)*/

      t4.*,
      t3.cate_level2_id
    FROM
      t_base_ec_dim t3

      JOIN
      (
        SELECT
          item_id,
          root_cat_id,
          cast(cat_id AS BIGINT),
          brand_id
        FROM
          t_base_ec_item_dev
        WHERE ds = '20160225'
      ) t4
        ON t4.cat_id = t3.cate_id
  )
  t1

  JOIN
  (
    SELECT
      user_id,
      item_id,
      LENGTH(content) content_len,
      f_date,
      annoy
    FROM t_base_ec_item_feed_dev
    WHERE CAST(user_id AS INT) > 0

  ) t2 ON t1.item_id = t2.item_id;


CREATE TABLE t_zlj_userProfile_cat2_num
  AS

    SELECT
      user_id,
      concat_ws(' ', collect_set(cat2_num))
    FROM
      (
        SELECT
          user_id,
          concat_ws(':', CASE WHEN cate_level2_id IS NULL
            THEN '001'
                         ELSE cate_level2_id END, cast(count(1) AS STRING)) AS cat2_num
        FROM t_ec_zlj_userbuy_history
        WHERE user_id RLIKE '^\\d+$' AND cate_level2_id RLIKE '^\\d+$'
        GROUP BY user_id, cate_level2_id
      )
      t
    GROUP BY user_id;

#评论次数

# 评价 特征关联
CREATE TABLE t_zlj_userProfile_cat2_num_userInfo
  AS
    SELECT
      t1.*,
      t2.*
    FROM t_base_user_info_s t1 JOIN t_zlj_userProfile_cat2_num t2 ON t1.tb_id = t2.user_id;


CREATE TABLE t_zlj_userProfile_cat2_num_userInfo_gender_data
  AS
    SELECT concat_ws(' ', tgender, `_c1`)
    FROM t_zlj_userProfile_cat2_num_userInfo
    WHERE length(tgender) = 1;



CREATE TABLE t_zlj_userProfile_cat2_num_userInfo_agelevel_data AS
SELECT concat_ws(' ',cast(CASE WHEN tage >= 18 AND tage <= 24
  THEN 1
       WHEN tage >= 25 AND tage <= 29
         THEN 2
       WHEN tage >= 30 AND tage <= 34
         THEN 3
       WHEN tage >= 35 AND tage <= 39
         THEN 4
       WHEN tage >= 40 AND tage <= 49
         THEN 5
       WHEN tage >= 50 AND tage <= 59
         THEN 6
       END  as string),`_c1`)
FROM t_zlj_userProfile_cat2_num_userInfo
WHERE tage > 17 AND tage < 60;






# 格力
DROP TABLE t_zlj_tmp;
CREATE TABLE t_zlj_tmp
  AS
    SELECT t1.*
    FROM t_base_user_info_s t1 JOIN
      (
        SELECT user_id
        FROM t_base_ec_record_dev
        WHERE brand_id = 30645 AND ds > 20151130 AND ds < 21060101) t2 ON t1.tb_id = t2.user_id;

# 0       7116
# 1       7981
select tgender,count(1) from  t_zlj_tmp  where length(tgender)>0 group by tgender;

# 1108    5739    3625    1664    884     117
SELECT
  sum(CASE WHEN tage >= 18 AND tage <= 24
    THEN 1 END),

  sum(CASE WHEN tage >= 25 AND tage <= 29
    THEN 1 END),

  sum(CASE WHEN tage >= 30 AND tage <= 34
    THEN 1 END),

  sum(CASE WHEN tage >= 35 AND tage <= 39
    THEN 1 END),

  sum(CASE WHEN tage >= 40 AND tage <= 49
    THEN 1 END),

  sum(CASE WHEN tage >= 50 AND tage <= 59
    THEN 1 END)
from  t_zlj_tmp  where tage>0


# 上海    6466
# 云南    1760
# 内蒙古  335
# 北京    2816
# 吉林    564
# 四川    5022
# 天津    925
# 宁夏    139
# 安徽    4698
# 山东    4920
# 山西    1360
# 广东    17657
# 广西    3511
# 新疆    348
# 江苏    13322
# 江西    4724
# 河北    2997
# 河南    5339
# 浙江    12800
# 海南    690
# 湖北    6331
# 湖南    5956
# 甘肃    526
# 福建    4907
# 西藏    62
# 贵州    2344
# 辽宁    1452
# 重庆    3248
# 陕西    1931
# 青海    83
# 香港    2
# 黑龙江  749
select  provice ,count(1 ) from
(
select
  split(tloc,' ')[0]  as provice
  from t_zlj_tmp where  length(tloc)>0
)t group by provice