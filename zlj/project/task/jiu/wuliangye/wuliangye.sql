CREATE TABLE t_base_ec_record_dev_wine_0407
  AS
    SELECT
      item_id,
      title,
      feed_id,
      user_id,
      content_length,
      annoy,
      ds,
      datediff,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      bc_type,
      price,
      location
    FROM t_base_ec_record_dev_new
    WHERE cat_id in ( 50008144,50013052) and ds>20151230;


-- 4537002  泸州老窖


select id1 ,id2 ,w, t3.info as t1info ,t4.info as t2info from
(select t1.id1 ,t2.info,t1.id2,w  from  infer t1 join t2 emp on t1.id1=t2.id)
t3
  join emp t4 on  t3.id2=t4.id
;

CREATE TABLE t_base_ec_record_dev_wine_userid
  AS
    SELECT user_id
    FROM
      t_base_ec_record_dev_wine
    GROUP BY user_id;


-- 销售额

# 01      1639559 9564
# 11      6998646 44913
# 12      5773599 38724

create table t_base_ec_record_dev_wine_marketsale as
SELECT
  brand_id,brand_name,
  m,
  sum(sale) AS  sale,
  sum(sale_num) sale_num
FROM
  (
    SELECT
      cast(sum(price) AS INT) AS sale,
      count(1)                   sale_num,
      substring(ds, 0, 6)     AS m ,
      brand_id,brand_name

    FROM
      t_base_ec_record_dev_wine
    WHERE brand_id in(
      '4101168','4536492','58184073','4537002','649572351','39887589','3670389','8224326','4005853','611468464','43937783','18028782','11972125','107545','51462495','34042','4537006','4540695','911468846','4536485','4536490','56742','565376057','91968643','1001026294','113091884','18910881','112028730','5827242','102033197','30752','4536999','4536640','22121372','56670','13610545','14057635','46693129','4536992','188444440','4536417','750268619','5413560','107544','7326664','4536184','4536639','34137281','4533616','106096950','446636085','10453463','4535635','4067900','4535618','4533829','3293520','16632319','3266761','27879732','84473','4142891','626620221','155633073','3517872','271566676','43096241','39482539','64088949','3516041','4539140','4537293','189334505','146067303','828432933','50526263','602','3564346','8648602','3422124','582634923','13233308','661456672','118550485','10541348','104692524','8626000','83777901','895926020','291292693','4539760','62081899','284068878','201118392','27848487','595868423','361258182','713342356','4535629','111500','89116351','6757959','957252450','758104766','653690004','254398165','102678','9416077','14200873','16743421','369938672','972578141','3328080','3983634','277462309','308706919','4537297','74626407','9886836','8831883','3568189','4535620','3363712','30069694','20141','691756333','653736261','872222877','10472595','278380596','4536414','3275599','7076820','110291','4535609','9352155','3990321','283888428','9947804','107802','151642353','153057356','4540103','754332752','550826692','277604860','714978852','630184642','372624301','738542994','588390738','8103320','7307247','16583850','3986854','779588161','211360414','669874065','3827987','803890388','377642457','15791542','3441120','199670376','15332451','116266239','105793452','75609534','11331650','107547','771476736','976192862','31296621','1057352564','8633803','7962590','841916381','6811883','30558','4533628','40576629','4093770','16304553','641411000','23239508','3441043','4534999','136921051','133882673','30362626','24556494','3864639','972528678','3323295','241466867','7088987','653628554','25402324','11251751','853048015'
    )
    GROUP BY ds,brand_id,brand_name
  ) t
GROUP BY brand_id ,brand_name,m;

# ALL
# 01      46600283        317882
# 11      236193314       1185534
# 12      163228253       995667
#  concat_ws('|', collect_set(v)) as diminfos
# (
# SELECT
#   brand_id ,brand_name,
#
#   concat_ws('_',m,cast(sum(sale) as String) ,cast( sum(sale_num )as String)) as v ,
#   m,
#   cast(sum(sale) as String)   sale,
#   cast( sum(sale_num )as String) sale_num

CREATE TABLE t_zlj_wine_market_sale
  AS
    SELECT
      brand_id,
      brand_name,
      sum(CASE WHEN m = '11'
        THEN sale
          ELSE 0 END) AS sale11,
      sum(CASE WHEN m = '12'
        THEN sale
          ELSE 0 END) AS sale12,
      sum(CASE WHEN m = '01'
        THEN sale
          ELSE 0 END) AS sale01
    FROM
      (
        SELECT
          brand_id,
          brand_name,
          cast(sum(price) AS INT) AS sale,
          count(1)                   sale_num,
          substring(ds, 5, 2)     AS m

        FROM
          t_base_ec_record_dev_wine
        WHERE ds >= 20151101 AND ds < 20160201
        GROUP BY ds, brand_id, brand_name
      ) t
    GROUP BY brand_id, brand_name;


CREATE TABLE t_zlj_wine_market_sale_num
  AS
    SELECT
      brand_id,
      brand_name,
      sum(CASE WHEN m = '11'
        THEN sale_num
          ELSE 0 END) AS sale11,
      sum(CASE WHEN m = '12'
        THEN sale_num
          ELSE 0 END) AS sale12,
      sum(CASE WHEN m = '01'
        THEN sale_num
          ELSE 0 END) AS sale01
    FROM
      (
        SELECT
          brand_id,
          brand_name,
          cast(sum(price) AS INT) AS sale,
          count(1)                   sale_num,
          substring(ds, 5, 2)     AS m

        FROM
          t_base_ec_record_dev_wine
        WHERE ds >= 20151101 AND ds < 20160201
        GROUP BY ds, brand_id, brand_name
      ) t
    GROUP BY brand_id, brand_name;


# 十一月二次购买率
# 30906   182360
# 30906   182360  36926   210013  38367   216825
# 3428    28214   7336    56795   8659    64796   9701    70889   11037   79694   13000   90289   14508   98303   16732   110764  20686   132323  24013   150220  30906   182360      36926   210013  38367   216825
SELECT
sum(case when 01_times>1 then 1 else 0 end ) AS 01_times_2_num,
sum(case when 01_times>0 then 1 else 0 end ) AS 01_num,
sum(case when 02_times>1 then 1 else 0 end ) AS 02_times_2_num,
sum(case when 02_times>0 then 1 else 0 end ) AS 02_num,
sum(case when 03_times>1 then 1 else 0 end ) AS 03_times_2_num,
sum(case when 03_times>0 then 1 else 0 end ) AS 03_num,
sum(case when 04_times>1 then 1 else 0 end ) AS 04_times_2_num,
sum(case when 04_times>0 then 1 else 0 end ) AS 04_num,
sum(case when 05_times>1 then 1 else 0 end ) AS 05_times_2_num,
sum(case when 05_times>0 then 1 else 0 end ) AS 05_num,
sum(case when 06_times>1 then 1 else 0 end ) AS 06_times_2_num,
sum(case when 06_times>0 then 1 else 0 end ) AS 06_num,
sum(case when 07_times>1 then 1 else 0 end ) AS 07_times_2_num,
sum(case when 07_times>0 then 1 else 0 end ) AS 07_num,
sum(case when 08_times>1 then 1 else 0 end ) AS 08_times_2_num,
sum(case when 08_times>0 then 1 else 0 end ) AS 08_num,
sum(case when 09_times>1 then 1 else 0 end ) AS 09_times_2_num,
sum(case when 09_times>0 then 1 else 0 end ) AS 09_num,
sum(case when 10_times>1 then 1 else 0 end ) AS 10_times_2_num,
sum(case when 10_times>0 then 1 else 0 end ) AS 10_num,
sum(case when 11_times>1 then 1 else 0 end ) AS 11_times_2_num,
sum(case when 11_times>0 then 1 else 0 end ) AS 11_num,
sum(case when 12_times>1 then 1 else 0 end ) AS 12_times_2_num,
sum(case when 12_times>0 then 1 else 0 end ) AS 12_num,
sum(case when 011_times>1 then 1 else 0 end ) AS 011_times_2_num ,
sum(case when 011_times>0 then 1 else 0 end ) AS 011_num
FROM
  (
    SELECT
      user_id,

      sum(case when m<201502     THEN 1       ELSE 0 END) AS 01_times,
      sum(case when m<201503     THEN 1       ELSE 0 END) AS 02_times,
      sum(case when m<201504     THEN 1       ELSE 0 END) AS 03_times,
      sum(case when m<201505     THEN 1       ELSE 0 END) AS 04_times,
      sum(case when m<201506     THEN 1       ELSE 0 END) AS 05_times,
      sum(case when m<201507     THEN 1       ELSE 0 END) AS 06_times,
      sum(case when m<201508     THEN 1       ELSE 0 END) AS 07_times,
      sum(case when m<201509     THEN 1       ELSE 0 END) AS 08_times,
      sum(case when m<201510     THEN 1       ELSE 0 END) AS 09_times,
      sum(case when m<201511     THEN 1       ELSE 0 END) AS 10_times,
      sum(case when m<201512     THEN 1       ELSE 0 END) AS 11_times,
      sum(case when m<201601     THEN 1       ELSE 0 END) AS 12_times,
      sum(case when m<201602     THEN 1       ELSE 0 END) AS 011_times
    FROM
      (
        SELECT
          *,
          substring(ds, 0, 6) AS m

        FROM t_base_ec_record_dev_wine
        WHERE brand_id =   4537002
      ) t1
    GROUP BY user_id
  ) t ;




# 3.2
# 182360  401223
# 182360  425233  182360  425233  182360  425233
# 182360  498015  210013  498015  216825  498015
SELECT
  sum(CASE WHEN brand_id = 4537002 and 11_times>0    THEN 1       ELSE 0 END) as 11_buy,
  count(CASE WHEN                      11_times>0    THEN 1       ELSE 0 END) as 11_all_buy,
    sum(CASE WHEN brand_id = 4537002 and 12_times>0    THEN 1       ELSE 0 END) as 12_buy,
  count(CASE WHEN                       12_times>0    THEN 1       ELSE 0 END) as 12_all_buy,
    sum(CASE WHEN brand_id = 4537002 and 01_times>0    THEN 1       ELSE 0 END) as 01_buy,
      count(CASE WHEN                   01_times>0    THEN 1       ELSE 0 END) as 01_all_buy
FROM

  (
    SELECT
      t1.user_id,
      brand_id,
      brand_name,
      sum(case when m<201512     THEN 1       ELSE 0 END) AS 11_times,
      sum(case when m<201601     THEN 1       ELSE 0 END) AS 12_times,
      sum(case when m<201602     THEN 1       ELSE 0 END) AS 01_times
    FROM
      (

        SELECT
          user_id,
          brand_id,
          brand_name,
          substring(ds, 0, 6) AS m
        FROM t_base_ec_record_dev_wine

      ) t1
      JOIN (
             SELECT
               DISTINCT user_id
             FROM
               t_base_ec_record_dev_wine
             WHERE brand_id = 4537002
           ) t2 ON t1.user_id = t2.user_id

    GROUP BY t1.user_id, t1.brand_id, t1.brand_name
  ) t;



# 28214   50102   58046   100951  66937   121396  73842   139211  83658   163639  95804   198108  105246  226851  119600  266562  144427  327277  165258  376423      202624  471024
SELECT
  sum(CASE WHEN brand_id = 4537002 AND m < 201502    THEN 1      ELSE 0 END) m201502,
  sum(CASE WHEN m < 201502    THEN 1      ELSE 0 END) ma201502,
  sum(CASE WHEN brand_id = 4537002 AND m < 201503    THEN 1      ELSE 0 END) m201503,
  sum(CASE WHEN m < 201503    THEN 1       ELSE 0 END) ma201503,
  sum(CASE WHEN brand_id = 4537002 AND m < 201504
    THEN 1
      ELSE 0 END) m201504,
  sum(CASE WHEN m < 201504
    THEN 1
      ELSE 0 END) ma201504,
  sum(CASE WHEN brand_id = 4537002 AND m < 201505
    THEN 1
      ELSE 0 END) m201505,
  sum(CASE WHEN m < 201505
    THEN 1
      ELSE 0 END) ma201505,
  sum(CASE WHEN brand_id = 4537002 AND m < 201506
    THEN 1
      ELSE 0 END) m201506,
  sum(CASE WHEN m < 201506
    THEN 1
      ELSE 0 END) ma201506,
  sum(CASE WHEN brand_id = 4537002 AND m < 201507
    THEN 1
      ELSE 0 END) m201507,
  sum(CASE WHEN m < 201507
    THEN 1
      ELSE 0 END) ma201507,
  sum(CASE WHEN brand_id = 4537002 AND m < 201508
    THEN 1
      ELSE 0 END) m201508,
  sum(CASE WHEN m < 201508
    THEN 1
      ELSE 0 END) ma201508,
  sum(CASE WHEN brand_id = 4537002 AND m < 201509
    THEN 1
      ELSE 0 END) m201509,
  sum(CASE WHEN m < 201509
    THEN 1
      ELSE 0 END) ma201509,
  sum(CASE WHEN brand_id = 4537002 AND m < 201510
    THEN 1
      ELSE 0 END) m201510,
  sum(CASE WHEN m < 201510
    THEN 1
      ELSE 0 END) ma201510,
  sum(CASE WHEN brand_id = 4537002 AND m < 201511
    THEN 1
      ELSE 0 END) m201511,
  sum(CASE WHEN m < 201511
    THEN 1
      ELSE 0 END) ma201511,
  sum(CASE WHEN brand_id = 4537002 AND m < 201512
    THEN 1
      ELSE 0 END) m201512,
  sum(CASE WHEN m < 201512
    THEN 1
      ELSE 0 END) ma201512,
  sum(CASE WHEN brand_id = 4537002 AND m < 201601
    THEN 1
      ELSE 0 END) m201601,
  sum(CASE WHEN m < 201601
    THEN 1
      ELSE 0 END) ma201601,
  sum(CASE WHEN brand_id = 4537002 AND m < 201602
    THEN 1
      ELSE 0 END) m201602,
  sum(CASE WHEN m < 201602
    THEN 1
      ELSE 0 END) ma201602
FROM

  (
    SELECT
      t1.user_id,
      brand_id,
      brand_name,
      m
    FROM
      (

        SELECT
          user_id,
          brand_id,
          brand_name,
          substring(ds, 0, 6) AS m
        FROM t_base_ec_record_dev_wine
        WHERE ds < 20160201

      ) t1
      JOIN (
             SELECT
               DISTINCT user_id
             FROM
               t_base_ec_record_dev_wine
             WHERE ds < 20160201 AND brand_id = 4537002
           ) t2 ON t1.user_id = t2.user_id

    GROUP BY t1.user_id, t1.brand_id, t1.brand_name, t1.m
  ) t;


# 3.3
CREATE TABLE t_zlj_wine_market_other_brand_01
  AS
    SELECT
      brand_id,
      brand_name,
      count(1)        AS num,
      sum(CASE WHEN m < 201501
        THEN 1
          ELSE 0 END) AS m201501,
      sum(CASE WHEN m < 201502
        THEN 1
          ELSE 0 END) AS m201502,
      sum(CASE WHEN m < 201503
        THEN 1
          ELSE 0 END) AS m201503,
      sum(CASE WHEN m < 201504
        THEN 1
          ELSE 0 END) AS m201504,
      sum(CASE WHEN m < 201505
        THEN 1
          ELSE 0 END) AS m201505,
      sum(CASE WHEN m < 201506
        THEN 1
          ELSE 0 END) AS m201506,
      sum(CASE WHEN m < 201507
        THEN 1
          ELSE 0 END) AS m201507,
      sum(CASE WHEN m < 201508
        THEN 1
          ELSE 0 END) AS m201508,
      sum(CASE WHEN m < 201509
        THEN 1
          ELSE 0 END) AS m201509,
      sum(CASE WHEN m < 201510
        THEN 1
          ELSE 0 END) AS m201510,
      sum(CASE WHEN m < 201511
        THEN 1
          ELSE 0 END) AS m201511,
      sum(CASE WHEN m < 201512
        THEN 1
          ELSE 0 END) AS m201512,
      sum(CASE WHEN m < 201601
        THEN 1
          ELSE 0 END) AS m201601,
      sum(CASE WHEN m < 201602
        THEN 1
          ELSE 0 END) AS m201602
    FROM
      (
        SELECT
          t1.user_id,
          brand_id,
          brand_name,
          m
        FROM
          (
            SELECT
              user_id,
              brand_id,
              brand_name,
              substring(ds, 0, 6) AS m
            FROM t_base_ec_record_dev_wine
            WHERE ds < 20160201
          ) t1
          JOIN (
                 SELECT
                   DISTINCT user_id
                 FROM
                   t_base_ec_record_dev_wine
                 WHERE ds < 20160201 AND brand_id = 4537002
               ) t2 ON t1.user_id = t2.user_id

        GROUP BY t1.user_id, t1.brand_id, t1.brand_name, t1.m
      ) t
    GROUP BY brand_id, brand_name;


# 4
# 201501  32651
# 201502  33896
# 201503  9971
# 201504  7763
# 201505  11084
# 201506  14068
# 201507  10953
# 201508  16599
# 201509  28636
# 201510  24281
# 201511  44913
# 201512  38724
# 201601  9564
SELECT
  m,
  count(1)
FROM
  (
    SELECT
      user_id,
      brand_id,
      brand_name,
      substring(ds, 0, 6) AS m
    FROM t_base_ec_record_dev_wine
    WHERE brand_id = 4537002

  ) t
GROUP BY m;


#5

CREATE TABLE t_base_ec_record_dev_wine_feed_parse
  AS
    SELECT
      t1.item_id,
      t1.user_id,
      substring(ds, 0, 6) AS m,

      neg_pos,
      impr_c
    FROM
      (
        SELECT
          item_id,
          user_id,
          ds
        FROM t_base_ec_record_dev_wine
        WHERE brand_id = 4537002
      ) t1
      JOIN
      t_zlj_feed2015_parse_v5_1

      t2 ON t1.item_id = t2.item_id AND t1.user_id = t2.user_id;


CREATE TABLE t_base_ec_record_dev_wine_feed_parse_result  as
SELECT
  m,
  neg_pos,
  sum(num),
  concat_ws('|', collect_set(impr_c_num))
FROM
  (
    SELECT
      m,
      neg_pos,
      concat_ws('_', impr_c, cast(num AS STRING)) impr_c_num,
      impr_c,
      rn,
      num
    FROM
      (
        SELECT
          m,
          neg_pos,
          impr_c,
          num,
          ROW_NUMBER() OVER (PARTITION BY m, neg_pos ORDER BY num DESC) AS rn
        FROM
        (
        SELECT
        m, neg_pos, impr_c, COUNT(1) AS num
        FROM
        (
        SELECT
        m, split(impr_c, ':')[2] AS neg_pos, concat(split(impr_c, ':')[0], split(impr_c, ':')[3], split(impr_c, ':')[1]) AS impr_c

        FROM t_base_ec_record_dev_wine_feed_parse
        WHERE length(impr_c)>0
        AND impr_c NOT LIKE '%物流%' AND impr_c NOT LIKE '%服务%' AND impr_c NOT LIKE '%商品%'
        )t4
        WHERE            (CASE WHEN neg_pos='-1' AND impr_c LIKE '%好%' THEN 1 ELSE 0 END)=0
        GROUP BY m, neg_pos, impr_c

        )t
      ) t2
    WHERE rn < 20
  ) t3
GROUP BY m, neg_pos

LIMIT 100;