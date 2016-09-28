
-- 用户消费统计特征表

DROP TABLE IF EXISTS t_base_user_consum_statis_data_title;
CREATE TABLE t_base_user_consum_statis_data_title AS
  SELECT
    user_id,

sum(tmall_ratio_price)/sum(price) as tmall_ratio,
sum(beauty_ratio_price)/sum(price) as beauty_ratio,
sum(game_ratio_price)/sum(price) as game_ratio,
sum(edu_ratio_price)/sum(price) as edu_ratio,
sum(medical_ratio_price)/sum(price) as medical_ratio,
sum(fraud_num) as fraud_cnt

from

(

SELECT  user_id ,price ,
 case when bc_type='B' then price else '0' end  as  tmall_ratio_price,
 case when root_cat_name in ('彩妆/香水/美妆工具','个人护理/保健/按摩器材','洗护清洁剂/卫生巾/纸/香薰','俪人购(俪人购专用)') then price else '0' end  as beauty_ratio_price,
case when root_cat_name in ('网游装备/游戏币/帐号/代练','网络游戏点卡') then price else '0' end as game_ratio_price,
case when root_cat_name in ('书籍/杂志/报纸','教育培训') then price else '0' end  as edu_ratio_price,
case when root_cat_name in ('OTC药品/医疗器械/计生用品','传统滋补营养品','保健食品/膳食营养补充食品','家庭保健','中药饮片') then price else '0' end  as medical_ratio_price,
case when     (title like '%PS%证件照%'  or title like '%证件照%换底%' or title like '%抠图%' or title like '%抠图%' )
or ((title like '%代缴%' or title like '%综合缴费%' or title like '%代支付%')
and title not like '%宽带%' and sku not like '%电信%' and sku not like '%移动%' and sku not like '%联通%')
or (title like '%社保%' or title like '%公积金%' or title like '%挂靠%')
or (title like '%苹果%解锁%' and root_cat_name like '%本地化生活服务%')
or ((title like '%安卓%解锁%' or title like '%苹果%解锁%' ) and root_cat_name like '%本地化生活服务%')
or (title like '%银行%流水%' )
then 1 else 0 end   as fraud_num
from t_base_ec_record_dev_new
where ds='true'
 )
 t  group by user_id
HAVING  COUNT(1)<5000
  ;





t_base_ec_record_dev_new
-- SELECT COUNT(1) from t_base_user_consum_statis_data local_buycnt>4500;
