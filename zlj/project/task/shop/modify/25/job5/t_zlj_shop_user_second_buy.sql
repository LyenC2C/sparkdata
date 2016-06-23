-- 二次购买



CREATE TABLE t_zlj_shop_second_buy AS

  SELECT
    t1.*,
    t2.main_cat_name
  FROM
    (SELECT
       shop_id,
       sum(CASE WHEN num > 1
         THEN 1
           ELSE 0 END)                            AS buy_second,
       sum(CASE WHEN num = 1
         THEN 1
           ELSE 0 END)                            AS buy_first,
       round(sum(CASE WHEN num > 1
         THEN 1
                 ELSE 0 END) * 100 / COUNT(1), 2) AS sec_retio
     FROM
       (
         SELECT
           shop_id,
           tb_id,
           COUNT(1) AS num
         FROM t_zlj_shop_shop_user_level_verify

         GROUP BY shop_id, tb_id
       ) t
     GROUP BY shop_id
    ) t1
    JOIN t_base_shop_major_all t2 ON t1.shop_id = t2.shop_id;


-- SELECT  100*buy_second/(buy_second+buy_first)  as sec_retio,main_cat_name
-- FROM t_zlj_shop_second_buy  where main_cat_name='3C数码' limit 100;


-- shop_id =65525181 ;

-- 行业二次购买率

-- SELECT
--   main_cat_name,
--   avg(sec_retio) as avg_sec_retio
-- FROM
--   (
--     SELECT
--       100 * buy_second / (buy_second + buy_first) AS sec_retio,
--       main_cat_name
--     FROM t_zlj_shop_second_buy
--   ) t
-- GROUP BY main_cat_name;


--  行业平均购买

-- 3C数码  19.988754874787666
-- OTC药品/医疗器械/计生用品       12.503986565952784
-- 中药饮片        13.114754098360656
-- 书籍音像        19.63952977487412
-- 保险    100.0
-- 保险（汇金收费）        16.666666666666668
-- 俪人购(俪人购专用)      26.196107115283027
-- 全球购代购市场  29.865206080624027
-- 公益    19.09306890901363
-- 其他    27.27531395230162
-- 其他行业        18.87607069993896
-- 农业生产资料（农村淘宝专用）    35.00153486359207
-- 包装    59.380314846335885
-- 合作商家        68.23232323232324
-- 国内机票/国际机票/增值服务      11.916367502941517
-- 国货精品数码    15.132812589964061
-- 处方药  11.377777777777778
-- 外卖/外送/订餐(请勿发布)        31.061022528699638
-- 外卖/外送/订餐服务（垂直市场）  70.98328433259829
-- 家居用品        25.3568246280361
-- 家庭保健        15.830472405311252
-- 家装家饰        24.273635065941974
-- 房产/租房/新房/二手房/委托服务  45.79763290410066
-- 整车(经销商)    3.7765884451763414
-- 新车/二手车     18.190059805346063
-- 服饰鞋包        19.657456806820957
-- 母婴    29.101215867791502
-- 淘女郎  47.77777777777778
-- 淘宝农资        28.944201856912297
-- 游戏/话费       33.72515788324884
-- 玩乐/收藏       26.978438768526313
-- 珠宝/配饰       27.155976845384924
-- 生活服务        29.697591293483423
-- 美容护理        30.33204974294518
-- 自用闲置转让    22.624327956989248
-- 装修设计/施工/监理      31.93072524804165
-- 资产    29.429105786910327
-- 车品配件        15.695364789342134
-- 运动/户外       20.707166015901212
-- 零售O2O 7.142857142857143
-- 食品/保健       28.64273669790499