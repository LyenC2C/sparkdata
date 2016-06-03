
四川地区手机号

        23692751
tbid 空 21546999

tbid     2145752



select item_id,Title,date,Store,shop_name,brand_id,Brand,Platform,Price,Sales_Amount,Sales_RMB,Category
from(
SELECT
  t1.item_id,
  t1.title         Title,
  t2.ds            date,
  t1.shop_id       Store,
  t1.shop_name,
  t1.brand_id,
  t1.brand_name    Brand,
  t1.bc_type       Platform,
  t2.day_sold_price       Price,
  t2.day_sold    Sales_Amount,
  t2.soldmo        Sales_RMB,
  t1.root_cat_name Category
FROM
  (SELECT
     r1.item_id,
     r1.title,
     r1.root_cat_name,
     r2.shop_name,
     r1.shop_id,
     r1.brand_id,
     r1.brand_name,
     r1.bc_type
   FROM (
          SELECT
            item_id,
            title,
            root_cat_name,
            shop_id,
            brand_id,
            brand_name,
            bc_type
          FROM t_base_ec_item_dev_new
          WHERE ds = '20160530'
                AND brand_name like '%Columbia%' or brand_name like '%Toread%'
                or brand_name like '%The North Face%' or brand_name like '%ELLE%'
                 or brand_name like '%Lesportsac%' or brand_name like '%Kipling%'
        ) r1
     JOIN
     (SELECT *
      FROM t_base_ec_shop_dev
       where ds = '20160330'
     )r2
       ON r1.shop_id = r2.shop_id
  )t1
  JOIN
  ( SELECT item_id, ds, day_sold, day_sold_price, cast(day_sold*day_sold_price AS FLOAT ) soldmo
    FROM t_base_ec_item_daysale_dev_new
    )t2
    ON t1.item_id = t2.item_id)i
    group by item_id,Title,date,Store,shop_name,brand_id,Brand,Platform,Price,Sales_Amount,Sales_RMB,Category
select * from t_base_ec_item_dev_new where ds=20160530 where price>65536 limit 1000 ;