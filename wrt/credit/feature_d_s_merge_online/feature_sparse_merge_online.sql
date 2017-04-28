-- --稀疏表合并
-- drop table wlcredit.t_wrt_credit_record_cate_feature_online;
-- create table wlcredit.t_wrt_credit_record_cate_feature_online as
-- select t1.tel_index,
-- concat_ws(' ',t1.feature,coalesce(t2.feature,''),coalesce(t3.feature,''),coalesce(t4.feature,''))
-- as feature
-- from
-- wlcredit.t_credit_record_cate1_feature_online t1
-- left JOIN
-- wlcredit.t_credit_record_cate2_feature_online t2
-- ON
-- t1.tel_index = t2.tel_index
-- left JOIN
-- wlcredit.t_credit_record_cate3_feature_online t3
-- ON
-- t1.tel_index = t3.tel_index
-- left JOIN
-- wlcredit.t_credit_record_cate4_feature_online t4
-- ON
-- t1.tel_index = t4.tel_index
-- ;


--稀疏表合并(cate1,cate2表分别用两个交叉特征months表代替)

drop table wl_feature.t_credit_sparse_features_online;
create table wl_feature.t_credit_sparse_features_online as
select t1.tel_index,
concat_ws(' ',t1.feature,coalesce(t2.feature,''),coalesce(t3.feature,''),coalesce(t4.feature,''))
as feature
from
wl_feature.t_credit_record_cate1_feature_months_online t1
left JOIN
wl_feature.t_credit_record_cate2_feature_months_online t2
ON
t1.tel_index = t2.tel_index
left JOIN
wl_feature.t_credit_record_cate3_feature_months_online t3
ON
t1.tel_index = t3.tel_index
left JOIN
wl_feature.t_credit_record_cate4_feature_months_online t4
ON
t1.tel_index = t4.tel_index
;
