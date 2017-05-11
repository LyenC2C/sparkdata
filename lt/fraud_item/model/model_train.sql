--====模型训练数据准备=====
--1)加标签
CREATE table wl_service.t_lt_train_items_v1 as
SELECT t1.*,case when t1.item_id=t2.item_id then 1 else 0 end as label from
(SELECT item_id,fscore_1,fscore_11,fscore_2,fscore_22,fscore_3,fscore_4,keywords,keywords_d from
wl_service.t_lt_base_tb_item_filter_words_all)t1
left JOIN
(SELECT item_id from wl_service.t_lt_base_tb_item_fraud_all)t2
on t1.item_id=t2.item_id;

--2)title words segment
CREATE TABLE wl_service.t_lt_train_item_words_v2 AS
SELECT t1.*,t2.words from
(SELECT item_id,label from wl_service.t_lt_train_items_v1)t1
JOIN
(select item_id,words from wl_analysis.t_lt_base_ciyun_item_segment_words where ds='20170428')t2
on t1.item_id=t2.item_id

--3)
SELECT count(1) FROM
(SELECT item_id from wl_service.t_lt_train_item_words_v2
where label=1)t;


--4)item info for model
CREATE TABLE t_lt_trian_item_info_v3 as
SELECT t1.item_id,fscore_1,fscore_11,fscore_2,fscore_22,fscore_3,fscore_4,
t2.cat_id,t2.root_cat_id,price,favor,shop_id,t1.label from
(SELECT * from wl_service.t_lt_train_items_v1)t1
JOIN
(SELECT item_id,cat_id,root_cat_id,price,favor,shop_id from wl_base.t_base_ec_item_dev_new where ds='20170429')t2
on t1.item_id=t2.item_id

--5)item info and shop info for model
CREATE table t_lt_trian_item_shop_info_v4 AS
select t1.item_id,fscore_1,fscore_11,fscore_2,fscore_22,fscore_3,fscore_4,
t1.cat_id,t1.root_cat_id,t1.price,t1.favor,star,credit,starts,item_count,fans_count,good_rate_p,desc_score,service_score,wuliu_score,
desc_highgap,service_highgap,wuliu_highgap,t1.label from
(SELECT * from t_lt_trian_item_info_v3)t1
left JOIN
(SELECT shop_id,star,credit,starts,item_count,fans_count,good_rate_p,desc_score,service_score,wuliu_score,
desc_highgap,service_highgap,wuliu_highgap from wl_base.t_base_ec_shop_dev_new where ds='20170429')t2
on t1.shop_id=t2.shop_id

--transform starts to timestamp
create table t_lt_trian_item_shop_info_v4_tmp as
select item_id,fscore_1,fscore_11,fscore_2,fscore_22,fscore_3,fscore_4,cat_id,root_cat_id,price,favor,
star,credit,item_count,fans_count,good_rate_p,desc_score,service_score,wuliu_score,
desc_highgap,service_highgap,wuliu_highgap,
unix_timestamp()-unix_timestamp(starts) as start_ts,label from
t_lt_trian_item_shop_info_v4)t2;

drop table t_lt_trian_item_shop_info_v4;
alter table t_lt_trian_item_shop_info_v4_tmp rename to t_lt_trian_item_shop_info_v4;


--======模型训练结果入库
--model result 0508
CREATE TABLE  if not exists wl_service.t_lt_train_model_result (
id STRING  COMMENT  '淘宝id',
label  STRING   COMMENT '原标签',
pre_label STRING  COMMENT '模型预测标签'
)
COMMENT '淘宝商品模型预测'
PARTITIONED by (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

--load
load data INPATH '/user/lt/model_0508/model_NB_keywords/part*' into TABLE wl_service.t_lt_train_model_result PARTITION (ds='model_NB_keywords');
load data INPATH '/user/lt/model_0508/model_NB_tfidf/part*' into TABLE wl_service.t_lt_train_model_result PARTITION (ds='model_NB_tfidf');
load data INPATH '/user/lt/model_0508/model_NB_tf/part*' into TABLE wl_service.t_lt_train_model_result PARTITION (ds='model_NB_tf');
load data INPATH '/user/lt/model_0508/model_LR_item/part*' into TABLE wl_service.t_lt_train_model_result PARTITION (ds='model_LR_item');
load data INPATH '/user/lt/model_0508/model_LR_shop/part*' into TABLE wl_service.t_lt_train_model_result PARTITION (ds='model_LR_shop');
load data INPATH '/user/lt/model_0508/model_LR_tf/part*' into TABLE wl_service.t_lt_train_model_result PARTITION (ds='model_LR_tf');

--analysis
create table wl_service.t_lt_train_model_result_check as
select t1.label,t1.pre_label,t2.* ,ds from
(select * from t_lt_train_model_result)t1
left join
(select title,(fscore_1+fscore_11+fscore_2+fscore_22+fscore_3+fscore_4) as fraud_score,keywords,item_id
from t_lt_base_tb_item_filter_words_all)t2
on t1.id=t2.item_id

select count(1) as num  from
(select * from t_lt_train_model_result_check where
ds='model_LR_tf')t

select count(1) as num  from
(select * from t_lt_train_model_result_check where
ds='model_NB_keywords' and pre_label='1.0')t


--======模型结果规则筛选===
--1.筛选
create table t_lt_model_result_filter_tmp as
select item_id from t_lt_train_model_result_check
where ds='model_NB_keywords' and pre_label='1.0'
and length(regexp_replace(title,'收藏|卡拉卡拉|打印纸|充值|热敏纸|收银|书',''))=length(title)
and keywords !='还款'
and keywords !='闪电'

--2.模型筛选商品信息
CREATE table t_lt_model_result_filter_tmp_2 as
select t2.* from
(select * from t_lt_model_result_filter_tmp)t1
join
(select item_id,title,cat_name,root_cat_name,
(fscore_1+fscore_11+fscore_2+fscore_22+fscore_3+fscore_4) as fraud_score,
keywords from t_lt_base_sp_item_filter_words_sub)t2
on t1.item_id=t2.item_id

--3.与规则筛选结果合并
insert overwrite table t_lt_base_sp_recom_item_fraud_all
select item_id,title,cat_name,root_cat_name,fraud_score,keywords from
(select * from t_lt_base_sp_recom_item_fraud_all
union all
select * from t_lt_model_result_filter_tmp_2)t
group by item_id,title,cat_name,root_cat_name,fraud_score,keywords



--========================
