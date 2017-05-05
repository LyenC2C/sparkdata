CREATE table wl_service.t_lt_train_items_v1 as
SELECT t1.*,case when t1.item_id=t2.item_id then 1 else 0 end as label from
(SELECT item_id,fraud_score_1,fraud_score_11,fraud_score_2,fraud_score_22,fraud_score_3,fraud_score_4,keywords from
wl_service.t_lt_base_sp_item_filter_words_sub)t1
left JOIN
(SELECT item_id from wl_service.t_lt_base_sp_item_fraud_all)t2
on t1.item_id=t2.item_id;

--2)title words fro model
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


--4)item info for mdoel
CREATE TABLE t_lt_trian_item_info_v3 as
SELECT t1.item_id,t1.fraud_score_1,t1.fraud_score_11,t1.fraud_score_2,t1.fraud_score_22,t1.fraud_score_3,t1.fraud_score_4,
t2.cat_id,t2.root_cat_id,price,favor,shop_id,t1.label from
(SELECT * from wl_service.t_lt_train_items_v1)t1
JOIN
(SELECT item_id,cat_id,root_cat_id,price,favor,shop_id from wl_base.t_base_ec_item_dev_new where ds='20170429')t2
on t1.item_id=t2.item_id

--5)item info and shop info for model
CREATE table t_lt_trian_item_shop_info_v4 AS
select t1.item_id,t1.fraud_score_1,t1.fraud_score_11,t1.fraud_score_2,t1.fraud_score_22,t1.fraud_score_3,t1.fraud_score_4,
t1.cat_id,t1.root_cat_id,t1.price,t1.favor,star,credit,starts,item_count,fans_count,good_rate_p,desc_score,service_score,wuliu_score,
desc_highgap,service_highgap,wuliu_highgap,t1.label from
(SELECT * from t_lt_trian_item_info_v3)t1
left JOIN
(SELECT shop_id,star,credit,starts,item_count,fans_count,good_rate_p,desc_score,service_score,wuliu_score,
desc_highgap,service_highgap,wuliu_highgap from wl_base.t_base_ec_shop_dev_new where ds='20170429')t2
on t1.shop_id=t2.shop_id







--model train :fraud_score info
create table t_lt_base_item_fraud_score_re_sp as
select * from t_lt_base_recommend_item_fraud_score
where (fraud_score_1>0) or (fraud_score_11>0) or (fraud_score_2>0) or (fraud_score_22>0)
or (fraud_score_3>0) or (fraud_score_4>0)
union all
select * from t_lt_base_sp_item_fraud_score_sub

--model trian with item info
create table t_lt_base_sp_item_fraud_train_v1 as
SELECT t1.*,t2.bc_type,t2.price,t2.favor,t2.shop_id,t2.root_cat_id from
(select * from t_lt_base_sp_item_fraud_score_sub)t1
left join
(SELECT item_id,bc_type,price,favor,shop_id,root_cat_id from wl_base.t_base_ec_item_dev_new where ds='20170415')t2
on t1.item_id=t2.item_id

create table wl_service.t_lt_base_sp_item_fraud_v2 as
select t1.*,t2.star,t2.credit,t2.starts,t2.item_count,t2.fans_count,
t2.good_rate_p, t2.desc_highgap,t2.service_highgap,t2.wuliu_highgap from
(select * from wl_service.t_lt_base_sp_item_fraud_v1)t1
left join
(select shop_id,star,credit,starts,item_count,fans_count,
good_rate_p, desc_highgap,service_highgap,wuliu_highgap
from wl_base.t_base_ec_shop_dev_new where ds='20170415')t2
on t1.shop_id=t2.shop_id

create table t_lt_base_sp_item_fraud_train as
select item_id,fraud_score_1,fraud_score_11,fraud_score_2,fraud_score_22,fraud_score_3,fraud_score_4,
keywords,bc_type,price,favor,root_cat_id,star,credit,starts,item_count,fans_count,good_rate_p,desc_highgap,
service_highgap,wuliu_highgap,
case when t1.item_id=t2.item_id then 1 else 0 end as label
from
(select * from t_lt_base_sp_item_fraud_v2)t1
left join
(select item_id from t_lt_base_sp_item_fraud_all)t2
on t1.item_id=t2.item_id

--analysis seller_id
create table wl_service.t_lt_base_fraud_seller_info as
select t1.*,t2.item_count,(t2.item_count/t1.fraud_item_num) as fraud_ratio from
(select seller_id,count(1) as fraud_item_num from
wl_service.t_lt_base_sp_item_fraud_all group by seller_id)t1
left join
(select seller_id,item_count from wl_base.t_base_ec_shop_dev_new
where ds='20170424')t2
on t1.seller_id=t2.seller_id;

CREATE table wl_service.t_lt_base_fraud_seller_item as
SELECT t2.* from
(SELECT seller_id from wl_service.t_lt_base_fraud_seller_info
	where fraud_ratio<11)t1
join
(select * from wl_base.t_base_ec_item_dev_new
where ds='20170424')t2
on t1.seller_id=t2.seller_id



--analysis weibo and taobao fraud user
create table t_lt_fraud_weibo_2_seller_info as
SELECT t1.snwb,t2.name,t2.description,t1.seller_id,t3.shop_id,t3.shop_name,t3.seller_name from
(SELECT snwb,seller_id FROM wl_service.t_lt_fraud_weibo_2_seller_id
where snwb is not null and seller_id is not null group by snwb,seller_id)t1
left join
(SELECT id,name,description from wl_analysis.t_lt_weibo_fraud_usr)t2
on t1.snwb=t2.id
left join
(SELECT seller_id,shop_id,shop_name,seller_name from wl_base.t_base_ec_shop_dev_new where ds='20170424')t3
on t1.seller_id=t3.seller_id


--num_shop
create table wl_service.t_lt_base_ec_seller_shop_count as
select seller_id,count(1) as num_shop from wl_base.t_base_ec_shop_dev_new
where ds='20170415'
group by seller_id having num_shop>1