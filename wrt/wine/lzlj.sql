1:

SELECT
  sum(day_sold),
  ds
FROM t_wrt_baijiu_itemsale
GROUP BY ds

2:

SELECT
  sum(t2.day_sold),
  t2.ds
FROM
  (SELECT item_id
   FROM t_wrt_baijiu_iteminfo
   WHERE brand_id = 4537002) t1
  JOIN
  (SELECT
     item_id,
     day_sold,
     ds
   FROM t_wrt_baijiu_itemsale) t2
    ON
      t1.item_id = t2.item_id
GROUP BY t2.ds;

3:

SELECT
  sum(day_sold_price),
  ds
FROM t_wrt_baijiu_itemsale
GROUP BY ds

4:
SELECT
  sum(t2.day_sold_price),
  t2.ds
FROM
  (SELECT item_id
   FROM t_wrt_baijiu_iteminfo
   WHERE brand_id = 4537002) t1
  JOIN
  (SELECT
     item_id,
     day_sold_price,
     ds
   FROM t_wrt_baijiu_itemsale) t2
    ON
      t1.item_id = t2.item_id
GROUP BY t2.ds;

5.
SELECT
  sum(day_sold_price) / sum(day_sold),
  ds
FROM t_wrt_baijiu_itemsale
GROUP BY ds

6.
SELECT
  sum(t2.day_sold_price) / sum(t2.day_sold),
  t2.ds
FROM
  (SELECT item_id
   FROM t_wrt_baijiu_iteminfo
   WHERE brand_id = 4537002) t1
  JOIN
  (SELECT
     item_id,
     day_sold_price,
     day_sold,
     ds
   FROM t_wrt_baijiu_itemsale) t2
    ON
      t1.item_id = t2.item_id
GROUP BY t2.ds;

7.
SELECT
  id,
  name,
  sold,
  sales
FROM
  (
    SELECT
      t1.brand_id        AS id,
      max(t1.brand_name) AS name,
      sum(t2.sold)       AS sold,
      sum(t2.sales)      AS sales
    FROM
      (SELECT
         item_id,
         brand_id,
         brand_name
       FROM t_wrt_baijiu_iteminfo) t1
      JOIN
      (SELECT
         item_id,
         sum(day_sold)       AS sold,
         sum(day_sold_price) AS sales
       FROM t_wrt_baijiu_itemsale
       WHERE ds > 20160300
       GROUP BY item_id) t2
        ON
          t1.item_id = t2.item_id
    GROUP BY brand_id
  ) t
ORDER BY sales DESC
LIMIT 20;

16.
SELECT
  t1.xiangxing AS xiangxing,
  sum(t2.sold) AS sold
FROM
  (SELECT
     item_id,
     xiangxing
   FROM t_wrt_baijiu_iteminfo
   WHERE brand_id = 4537002) t1
  JOIN
  (SELECT
     item_id,
     sum(day_sold) AS sold
   FROM t_wrt_baijiu_itemsale
   WHERE ds > 20160300
   GROUP BY item_id) t2
    ON
      t1.item_id = t2.item_id
GROUP BY xiangxing;

17.
SELECT
  t1.xiangxing AS dushu,
  sum(t2.sold) AS sold
FROM
  (SELECT
     item_id,
     dushu
   FROM t_wrt_baijiu_iteminfo
   WHERE brand_id = 4537002) t1
  JOIN
  (SELECT
     item_id,
     sum(day_sold) AS sold
   FROM t_wrt_baijiu_itemsale
   WHERE ds > 20160300
   GROUP BY item_id) t2
    ON
      t1.item_id = t2.item_id
GROUP BY dushu;

18.
SELECT
  CASE
  WHEN t1.price > 0 AND t1.price <= 150 THEN '0-150'
  WHEN t1.price > 150 AND t1.price <= 300 THEN '150-300'
  WHEN t1.price > 300 AND t1.price <= 500 THEN '300-500'
  WHEN t1.price > 500 AND t1.price <= 600 THEN '500-600'
  WHEN t1.price > 600 AND t1.price <= 800 THEN '600-800'
  WHEN t1.price > 800 THEN '800-'
  END price_level,
  sum(t2.sold)
FROM
  (SELECT
     item_id,
     price
   FROM t_wrt_baijiu_iteminfo
   WHERE brand_id = 4537002) t1
  JOIN
  (SELECT
     item_id,
     sum(day_sold) AS sold
   FROM t_wrt_baijiu_itemsale
   WHERE ds > 20160300
   GROUP BY item_id) t2
    ON
      t1.item_id = t2.item_id
GROUP BY
  CASE
  WHEN t1.price > 0 AND t1.price <= 150 THEN '0-150'
  WHEN t1.price > 150 AND t1.price <= 300 THEN '150-300'
  WHEN t1.price > 300 AND t1.price <= 500 THEN '300-500'
  WHEN t1.price > 500 AND t1.price <= 600 THEN '500-600'
  WHEN t1.price > 600 AND t1.price <= 800 THEN '600-800'
  WHEN t1.price > 800 THEN '800-'
  END;

19.
SELECT
  jinghan,
  sum(sold)
FROM
  (SELECT
     CASE
     WHEN t1.jinghan LIKE "%m%" THEN cast(split(t1.jinghan, 'm')[0] AS INT)
     WHEN t1.jinghan LIKE "%M%" THEN cast(split(t1.jinghan, 'M')[0] AS INT)
     WHEN t1.jinghan LIKE "%L%" THEN split(t1.jinghan, 'L')[0] * 1000
     WHEN t1.jinghan LIKE "%l%" THEN split(t1.jinghan, 'l')[0] * 1000
     END jinghan,
     sold
   FROM
     (SELECT
        item_id,
        jinghan
      FROM t_wrt_baijiu_iteminfo
      WHERE brand_id = 4537002) t1
     JOIN
     (SELECT
        item_id,
        sum(day_sold) AS sold
      FROM t_wrt_baijiu_itemsale
      WHERE ds > 20160300
      GROUP BY item_id) t2
       ON
         t1.item_id = t2.item_id
  ) t
GROUP BY jinghan

21.
SELECT
  ttt1.shop_id,
  ttt2.shop_name,
  ttt1.sold,
  ttt1.sales,
  ttt1.loc,
  ttt1.brand_count
FROM
  (
    SELECT
      shop_id,
      sold,
      sales,
      loc,
      brand_count
    FROM
      (
        SELECT
          tt1.shop_id,
          tt1.sold,
          tt1.sales,
          tt1.loc,
          tt2.brand_count
        FROM
          (
            SELECT
              t1.shop_id       AS shop_id,
              sum(t2.sold)     AS sold,
              sum(t2.sales)    AS sales,
              max(t1.location) AS loc
              FROM
              (SELECT
                 item_id,
                 shop_id,
                 location
               FROM t_wrt_baijiu_iteminfo where brand_id = 4537002) t1
              JOIN
              (SELECT
                 item_id,
                 sum(day_sold)       AS sold,
                 sum(day_sold_price) AS sales
               FROM t_wrt_baijiu_itemsale
               WHERE ds > 20160300
               GROUP BY item_id) t2
                ON
                  t1.item_id = t2.item_id
            GROUP BY
              t1.shop_id
          ) tt1
          JOIN
          (
            SELECT
              shop_id,
              count(brand_id) AS brand_count
            FROM
              (
                SELECT
                  shop_id,
                  brand_id
                FROM t_wrt_baijiu_iteminfo
                GROUP BY shop_id, brand_id
              ) t
            GROUP BY shop_id
          ) tt2
            ON
              tt1.shop_id = tt2.shop_id
      ) tt
    ORDER BY sold DESC
    LIMIT 10
  ) ttt1
  JOIN
  (SELECT
     shop_id,
     shop_name
   FROM t_wrt_baijiu_shopinfo) ttt2
    ON
      ttt1.shop_id = ttt2.shop_id;

24/25.
select item_id,shop_id,shop_name,title,p from
(
  SELECT t1.item_id, t2.shop_id, t2.shop_name, t1.title, cast(t1.price as int) as p FROM
(SELECT * FROM t_wrt_baijiu_iteminfo WHERE brand_id = 4537002)t1
JOIN
(SELECT shop_id, shop_name FROM t_wrt_baijiu_shopinfo)t2
ON
t1.shop_id = t2.shop_id
)t
ORDER BY p limit 200;

top50:


SELECT
  tt1.price_zone,
  sold,
  name,
  tt2.item_id
FROM
  (SELECT
     price_zone,
     sum(day_sold) AS sold,
     max(favor)    AS name
   FROM
     (
       SELECT
         t1.item_id,
         t1.price_zone,
         t1.favor,
         t2.day_sold
       FROM
         (SELECT
            item_id,
            price_zone,
            favor
          FROM t_wrt_baijiu_iteminfo
          WHERE brand_id = 4537002) t1
         JOIN
         (SELECT
            item_id,
            sum(day_sold) as day_sold
          FROM t_wrt_baijiu_itemsale group by item_id) t2
           ON
             t1.item_id = t2.item_id
     ) t
   GROUP BY price_zone) tt1
  JOIN
  (
      SELECT  item_id,price_zone FROM
      (SELECT
        item_id,
        price_zone,
        ROW_NUMBER() OVER (PARTITION BY price_zone ORDER BY day_sold DESC) AS rn
      FROM

      (
      SELECT t1.item_id, t1.price_zone, t2.day_sold FROM
      (SELECT item_id, price_zone FROM t_wrt_baijiu_iteminfo WHERE brand_id = 4537002)t1
      JOIN
      (SELECT
            item_id,
            sum(day_sold) as day_sold
          FROM t_wrt_baijiu_itemsale group by item_id)t2
      ON
      t1.item_id = t2.item_id
      )t

      )tttt

      WHERE rn = 1
      GROUP BY item_id, price_zone

  ) tt2
    ON
      tt1.price_zone = tt2.price_zone
    order by sold desc limit 50;


zlj:
select t1.item_id,t2.sold from
(select item_id from t_wrt_baijiu_iteminfo where brand_id = 4537002)t1
join
(select item_id,sum(day_sold)as sold from t_wrt_baijiu_itemsale group by item_id)t2
on
t1.item_id = t2.item_id
order by sold desc limit 10;


#不同容量区间的销量、销售额、客单价、品牌分布:
select t1.jinghan_level,sum(t2.sold) as sales from
(
  SELECT
    CASE
    WHEN t.jinghan > 0 AND t.jinghan <= 500 THEN '0-500'
    WHEN t.jinghan > 500 AND t.jinghan <= 1000 THEN '500-1000'
    WHEN t.jinghan > 1000 AND t.jinghan <= 1500 THEN '1000-1500'
    WHEN t.jinghan > 1500 AND t.jinghan <= 2500 THEN '1500-2500'
    WHEN t.jinghan > 2500 THEN '2500-'
    END jinghan_level,item_id
  FROM
    (
      SELECT CASE WHEN jinghan LIKE "%m%" THEN cast(split(jinghan, 'm')[0] AS INT)
     WHEN jinghan LIKE "%M%" THEN cast(split(jinghan, 'M')[0] AS INT)
     WHEN jinghan LIKE "%L%" THEN split(jinghan, 'L')[0] * 1000
     WHEN jinghan LIKE "%l%" THEN split(jinghan, 'l')[0] * 1000
      END jinghan,item_id FROM t_wrt_baijiu_iteminfo
    ) t
)t1
join
(select item_id,sum(day_sold) as sold from t_wrt_baijiu_itemsale group by item_id)t2
on
t1.item_id = t2.item_id
group by t1.jinghan_level;


select t1.jinghan_level,sum(t2.sales) from
(
  SELECT
    CASE
    WHEN t.jinghan > 0 AND t.jinghan <= 500 THEN '0-500'
    WHEN t.jinghan > 500 AND t.jinghan <= 1000 THEN '500-1000'
    WHEN t.jinghan > 1000 AND t.jinghan <= 1500 THEN '1000-1500'
    WHEN t.jinghan > 1500 AND t.jinghan <= 2500 THEN '1500-2500'
    WHEN t.jinghan > 2500 THEN '2500-'
    END jinghan_level,item_id
  FROM
    (
      SELECT CASE WHEN jinghan LIKE "%m%" THEN cast(split(jinghan, 'm')[0] AS INT)
     WHEN jinghan LIKE "%M%" THEN cast(split(jinghan, 'M')[0] AS INT)
     WHEN jinghan LIKE "%L%" THEN split(jinghan, 'L')[0] * 1000
     WHEN jinghan LIKE "%l%" THEN split(jinghan, 'l')[0] * 1000
      END jinghan,item_id FROM t_wrt_baijiu_iteminfo
    ) t
)t1
join
(select item_id,sum(day_sold_price) as sales from t_wrt_baijiu_itemsale group by item_id)t2
on
t1.item_id = t2.item_id
group by t1.jinghan_level;

建新表：
create table t_wrt_baijiu_all like t_tc_baijiusaleok;
insert overwrite table t_wrt_baijiu_all
SELECT
  day_sold,
day_sold_price,
t1ds ,
item_id ,
title ,
cat_id ,
cat_name ,
wine_cate ,
is_jinkou ,
brand_id ,
brand_name ,
bc_type ,
xiangxing ,
dushu ,
  CASE WHEN jinghan LIKE "%m%" THEN cast(split(jinghan, 'm')[0] AS INT)
     WHEN jinghan LIKE "%M%" THEN cast(split(jinghan, 'M')[0] AS INT)
     WHEN jinghan LIKE "%L%" THEN split(jinghan, 'L')[0] * 1000
     WHEN jinghan LIKE "%l%" THEN split(jinghan, 'l')[0] * 1000
      END jinghan,
price ,
price_zone ,
favor ,
seller_id ,
shop_id ,
location ,
ts ,
ds
FROM t_tc_baijiusaleok

select prodfg,count(1) from (select split(tloc,' ')[0] as prodfg from t_xq_baijiuuser_new)t group by prodfg;


#new 2:
#五粮液：4536492,520673633190
#Moutai/茅台：4101168,40680546858
#泸州老窖：4537002,524860773192
#舍得：4537297,43371677392
#沱牌： 7076326,521468742514
#丰谷：9868031 FORGOOD/丰谷：806588915,12442757597

select item_id,sum(day_sold) from t_wrt_baijiu_all where brand_id =

select * from
(
SELECT
        item_id,
        brand_id,
        sold,
        Row_Number() OVER (PARTITION BY brand_id ORDER BY sold DESC )AS rn
      FROM
(
SELECT item_id,brand_id,sum(day_sold)as sold FROM t_wrt_baijiu_all WHERE
brand_id = 4536492 OR brand_id = 4101168 OR brand_id = 4537002 OR brand_id = 4537297 OR brand_id = 7076326 or
brand_id = 9868031 or brand_id = 806588915
group by item_id,brand_id
)t
)tt
where rn < 2;

#result:
40680546858	4101168	39166	1
520673633190	4536492	29985	1
524860773192	4537002	41964	1
43371677392	4537297	8386	1
521468742514	7076326	4710	1
520114119755	806588915	1173	1
12442757597	9868031	2223	1


select item_id,title,brand_name,dushu,jinghan,xiangxing from t_wrt_baijiu_iteminfo WHERE item_id IN
(
520673633190,
40680546858,
524860773192,
43371677392,
521468742514,
12442757597
);


new 4:
select shop_id,sold from
(
SELECT shop_id, sum(day_sold) as sold FROM t_wrt_baijiu_all WHERE brand_id = 9868031 OR brand_id = 806588915
GROUP BY shop_id
)t
order by sold desc limit 10;

select shop_id,shop_name from t_wrt_baijiu_shopinfo where shop_id in
(
119741482,
67653130,
112017793,
67597230,
110106942,
71233564,
106431294,
100300252)

#new 5:
#酒仙网店铺：63146329

select sum(day_sold),sum(day_sold_price) from t_wrt_baijiu_all where shop_id = 63146329;

select sum(day_sold),sum(day_sold_price) from t_wrt_baijiu_all where shop_id = 63146329 and

#new 6:
  select item_id,sold from
(select item_id,sum(day_sold) as sold from t_wrt_baijiu_all where cast(price as int) > 150.0 and cast(price as int) < 200.0
group by item_id)t
order by sold desc limit 1;

select item_id,sold from
(select item_id,sum(day_sold) as sold from t_wrt_baijiu_all where cast(price as int) > 150.0
and cast(price as int) < 200.0
and (brand_id = 9868031 or brand_id = 806588915)
group by item_id)t
order by sold desc limit 1;

select item_id,title,brand_name,dushu,jinghan,xiangxing from t_wrt_baijiu_iteminfo WHERE item_id IN
(
39116986411,
    45642533882
);


150到200块销量最好的商品及详情
39116986411	洋河海之蓝42度375ml 2瓶 洋河官方旗舰店 蓝色经典 绵柔型白酒	洋河	42	375mlX2瓶	浓香型
150到200块销量最好的丰谷商品及详情
45642533882	丰谷酒业 丰谷精品特曲 浓香型中度四川白酒  48度500ml	FORGOOD/丰谷	48	500ml	浓香型

