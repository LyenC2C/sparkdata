--密集表合并
wlcredit.t_credit_record_feature    zhubiao

wlcredit.t_credit_record_feature_6month

wlcredit.t_credit_user_profile_feature

drop table wlcredit.t_credit_dense_features
create table wlcredit.t_credit_dense_features as
SELECT
tt1.*,
tt2.alipay_flag,
tt2.buycnt,
tt2.verify_level,
tt2.regtime_month,
tt2.buy_freq,
tt2.tel_loc,
tt2.model_predict_gender
from
(
select
--由于是left join 所以某些特征值会生成null，后面python处理会处理掉。
t1.tel_index,
t1.price_sum,
t1.buy_count,
t1.price_avg,
t1.price_max,
t1.price_min,
t1.price_std,
t1.price_median,
t1.price_cross,
t1.price_025,
t1.price_075,
t1.price_010,
t1.annoy_num,
t1.annoy_ratio,
t1.brand_id_num,
t1.root_cat_id_num,
t1.b_bc_type_num,
t1.b_bc_type_num_ratio,
t1.b_bc_price_ratio,
t1.brand_effec_price_ratio,
t1.brand_effec_num_ratio,
t1.b50_num_ratio,
t1.b50_ratio,
t1.b30_num_ratio,
t1.b30_ratio,
t1.b10_num_ratio,
t1.b10_ratio,
t1.b5_num_ratio,
t1.b5_ratio,
t1.b50_10_num_ratio,
t1.b50_9_num_ratio,
t1.b50_8_num_ratio,
t1.b50_7_num_ratio,
t1.b50_6_num_ratio,
t1.b50_5_num_ratio,
t1.b50_4_num_ratio,
t1.b50_3_num_ratio,
t1.b50_2_num_ratio,
t1.b50_1_num_ratio,
t1.b50_0_num_ratio,
t1.b50_10_ratio,
t1.b50_9_ratio,
t1.b50_8_ratio,
t1.b50_7_ratio,
t1.b50_6_ratio,
t1.b50_5_ratio,
t1.b50_4_ratio,
t1.b50_3_ratio,
t1.b50_2_ratio,
t1.b50_1_ratio,
t1.b50_0_ratio,
t1.active_score,
t2.month6_price_sum,
t2.month6_buy_count,
t2.month6_price_avg,
t2.month6_price_max,
t2.month6_price_min,
t2.month6_price_std,
t2.month6_price_median,
t2.month6_price_cross,
t2.month6_price_025,
t2.month6_price_075,
t2.month6_price_010,
t2.month6_annoy_num,
t2.month6_annoy_ratio,
t2.month6_brand_id_num,
t2.month6_root_cat_id_num,
t2.month6_b_bc_type_num,
t2.month6_b_bc_type_num_ratio,
t2.month6_b_bc_price_ratio,
t2.month6_brand_effec_price_ratio,
t2.month6_brand_effec_num_ratio,
t2.month6_b50_num_ratio,
t2.month6_b50_ratio,
t2.month6_b30_num_ratio,
t2.month6_b30_ratio,
t2.month6_b10_num_ratio,
t2.month6_b10_ratio,
t2.month6_b5_num_ratio,
t2.month6_b5_ratio,
t2.month6_b50_10_num_ratio,
t2.month6_b50_9_num_ratio,
t2.month6_b50_8_num_ratio,
t2.month6_b50_7_num_ratio,
t2.month6_b50_6_num_ratio,
t2.month6_b50_5_num_ratio,
t2.month6_b50_4_num_ratio,
t2.month6_b50_3_num_ratio,
t2.month6_b50_2_num_ratio,
t2.month6_b50_1_num_ratio,
t2.month6_b50_0_num_ratio,
t2.month6_b50_10_ratio,
t2.month6_b50_9_ratio,
t2.month6_b50_8_ratio,
t2.month6_b50_7_ratio,
t2.month6_b50_6_ratio,
t2.month6_b50_5_ratio,
t2.month6_b50_4_ratio,
t2.month6_b50_3_ratio,
t2.month6_b50_2_ratio,
t2.month6_b50_1_ratio,
t2.month6_b50_0_ratio,
t2.month6_active_score
from
wlcredit.t_credit_record_feature t1
left JOIN
wlcredit.t_credit_record_feature_6month t2
ON
t1.tel_index = t2.tel_index
)tt1
left JOIN
wlcredit.t_credit_user_profile_feature tt2
ON
tt1.tel_index = tt2.tel_index;