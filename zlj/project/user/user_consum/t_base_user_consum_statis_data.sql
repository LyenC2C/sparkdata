
-- 用户消费统计特征表

DROP TABLE IF EXISTS t_base_user_consum_statis_data;
CREATE TABLE t_base_user_consum_statis_data AS
  SELECT
    user_id,
    sum(price)                  consume_price,
    avg(price) AS               avg_price,
    max(price) as max_price,
    min(price) as min_price ,
    count(1)   AS               local_buycnt,
    COUNT(DISTINCT cat_id)      cat_id_num,
    COUNT(DISTINCT root_cat_id) root_cat_id_num,
    COUNT(DISTINCT brand_id)    brand_id_num,
    sum(CASE WHEN length(brand_id) > 2      THEN 1         ELSE 0 END)             brand_effec_id_num,
    sum(CASE WHEN length(brand_id) <= 2       THEN 1        ELSE 0 END)             brand_no_effec_id_num,
    sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)             annoy_num,
    sum(CASE WHEN annoy = '0'       THEN 1        ELSE 0 END)             no_annoy_num,
    sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END)             b_bc_type_num,
    sum(CASE WHEN bc_type = 'C'       THEN 1         ELSE 0 END)             c_bc_type_num,
-- xiage
sum(case when bc_type='B' then price else '0' end)/sum(price) as tmall_ratio,
sum(case when root_cat_name in ('彩妆/香水/美妆工具','个人护理/保健/按摩器材','洗护清洁剂/卫生巾/纸/香薰','俪人购(俪人购专用)') then price else '0' end)/sum(price) as beauty_ratio,
sum(case when root_cat_name in ('网游装备/游戏币/帐号/代练','网络游戏点卡') then price else '0' end)/sum(price) as game_ratio,
sum(case when root_cat_name in ('书籍/杂志/报纸','教育培训') then price else '0' end)/sum(price) as edu_ratio,
sum(case when root_cat_name in ('OTC药品/医疗器械/计生用品','传统滋补营养品','保健食品/膳食营养补充食品','家庭保健','中药饮片') then price else '0' end)/sum(price) as medical_ratio,
sum(case when     (title like '%PS%证件照%'  or title like '%证件照%换底%' or title like '%抠图%' or title like '%抠图%' ) or ((title like '%代缴%' or title like '%综合缴费%' or title like '%代支付%') and title not like '%宽带%' and sku not like '%电信%' and sku not like '%移动%' and sku not like '%联通%')
or (title like '%社保%' or title like '%公积金%' or title like '%挂靠%')
or (title like '%苹果%解锁%' and root_cat_name like '%本地化生活服务%')
or ((title like '%安卓%解锁%' or title like '%苹果%解锁%' ) and root_cat_name like '%本地化生活服务%')
or (title like '%银行%流水%' )
then 1 else 0 end) as fraud_cnt,
sum(case when price <=50 then 1 else 0 end)/count(*) as b50_ratio,
sum(case when price <=100 and price>50 then 1 else 0 end)/count(*) as a50b100_ratio,
sum(case when price <=500 and price>100 then 1 else 0 end)/count(*) as a100b500_ratio,
sum(case when price <=1000 and price>500 then 1 else 0 end)/count(*) as a500b1000_ratio,
sum(case when price <=5000 and price>1000 then 1 else 0 end)/count(*) as a1000b5000_ratio,
sum(case when price <=10000 and price>5000 then 1 else 0 end)/count(*) as a5000b10000_ratio,
sum(case when price >10000 then 1 else 0 end)/count(*) as a10000_raio

from t_base_ec_record_dev_new
where ds='true' group by user_id

  ;

