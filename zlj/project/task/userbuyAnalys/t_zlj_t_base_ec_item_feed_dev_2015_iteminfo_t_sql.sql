--  2015 开始
-- 3009060480 条
CREATE TABLE t_zlj_t_base_ec_item_feed_dev_2015
  AS
    SELECT
      item_id,
      feed_id,
      user_id,
      annoy,
      ds,
      datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), f_date) AS datediff,
    FROM t_base_ec_item_feed_dev
    WHERE ds > 20150101;


SELECT * from (SELECT  item_id,count(1) as num from t_zlj_t_base_ec_item_feed_dev_2015 group by item_id)t ORDER BY  num desc limit 100;

# SELECT * ,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY f_date DESC) AS rn
# FROM (select * FROM t_zlj_t_base_ec_item_feed_dev_2015 limit 1000)t  ;

# select count(1) from t_zlj_t_base_ec_item_feed_dev_2015 where length(user_id)<2 ;
# select * from t_zlj_t_base_ec_item_feed_dev_2015 where item_id=524652527985 ;

# SELECT *
# FROM (SELECT item_id , count(1) as num from t_zlj_t_base_ec_item_feed_dev_2015 group by item_id ) t ORDER BY  num desc limit 100;
#
#
# SELECT *
# FROM (SELECT user_id , count(1) as num from t_zlj_t_base_ec_item_feed_dev_2015 group by user_id ) t ORDER BY  num desc limit 100;


# 2758157467
# 3009060480

set hive.map.aggr=true;
set hive.groupby.skewindata=true;
CREATE TABLE t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t
  AS
    SELECT
      t2.*,
      t1.title,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      bc_type,
      price,
      location
    FROM (SELECT
       cast( item_id as bigint )item_id,
     title,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      bc_type,
      price,
      location
          FROM t_base_ec_item_dev
          WHERE ds = 20160105 ) t1
      JOIN (
     SELECT
       cast( item_id as bigint )item_id,
       feed_id,
       user_id,
       length(content) content_length,
       annoy,
       ds,
       datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), f_date) AS datediff
     FROM t_base_ec_item_feed_dev
     WHERE ds > 20151212 and user_id is not NULL  and item_id is not null
           AND user_id NOT IN (
       21786683,
       2428807140,
       1934219124,
       1582724174,
       2266382677,
       2533976262,
       2171014314,
       641677535,
       882117473,
       669291177,
       2521130135,
       326330175,
       2519815747,
       2541363316,
       1015292634,
       2535441677,
       279230621,
       2539239285,
       111580834,
       1967985075,
       2535481590,
       2582666524,
       82453551,
       2582806041,
       2539169138,
       2177846033,
       2582705905,
       44513951,
       1743708559,
       2150641112,
       2540690600,
       2582566075,
       180760184,
       2323644755,
       1850371687,
       2535771523,
       30690824,
       73951552,
       316574966,
       870047975,
       134908689,
       927701626,
       440975649,
       712266043,
       21258611,
       1685224962,
       340552043,
       304843795,
       2503183578,
       2598181473,
       153420610,
       1659141549,
       200810912,
       2539129282,
       2541343224,
       2540992717,
       1727026267,
       2598271295,
       2541102161,
       1857701111,
       1101638978,
       2273913517,
       364893355,
       2503184984,
       896018004,
       2582735638,
       2345946537,
       2597265070,
       2547842429,
       2595172023,
       420692989,
       834104415,
       2503093409,
       17310403,
       2091541910,
       2138745740,
       2541002922,
       1034878917,
       828552108,
       832363636,
       2535831099,
       1065291365,
       2582585939,
       56097993,
       1928522790,
       2320358674,
       476669669,
       2344297211,
       2327010434,
       2145619492,
       1690479396,
       57980926,
       270077388,
       1644787520,
       290571277,
       316403543,
       359316154,
       677318689,
       2065032645,
       433109874
     )
           ) t2 ON t1.item_id = t2.item_id
  ;



# 统计以及类目比例
SELECT
  root_cat_id,
  count(1)
FROM t_zlj_t_base_ec_item_feed_dev_2015_iteminfo
GROUP BY root_cat_id;


SELECT *
FROM t_zlj_t_base_ec_item_feed_dev_2015_iteminfo
WHERE user_id = 379638601

--  叶子类目比重  均价

SELECT
  t2.*,
  t1.cate_name
FROM
  (
    SELECT
      cate_id,
      cate_name
    FROM t_base_ec_dim
    GROUP BY cate_id, cate_name
  ) t1
  JOIN
  (
    SELECT
      cat_id,
      count(1),
      cast(avg(price) AS INT) avg_price
    FROM t_zlj_t_base_ec_item_feed_dev_2015_iteminfo
    GROUP BY cat_id
  ) t2 ON t1.cate_id = t2.cat_id




# miss data

CREATE TABLE t_zlj_t_base_ec_item_feed_dev_2015_miss_id
  AS
    SELECT t2.item_id
    FROM t_base_ec_item_dev t1
      RIGHT JOIN t_zlj_t_base_ec_item_feed_dev_2015 t2
        ON t1.ds = 20151211 AND t1.item_id = t2.item_id AND t1.item_id IS NULL;


# cat leaf_cat   cast

SELECT
  user_id,
  cat_id,
  root_cat_id,
  sum(price) leafSumPirce
FROM t_zlj_t_base_ec_item_feed_dev_2015_iteminfo
GROUP BY user_id, cat_id, root_cat_id, cat_id