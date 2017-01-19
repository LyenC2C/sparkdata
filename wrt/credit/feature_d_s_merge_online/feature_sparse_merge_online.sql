--稀疏表合并
drop table wlcredit.t_wrt_credit_record_cate_feature_online;
create table wlcredit.t_wrt_credit_record_cate_feature_online as
select t1.tel_index,
concat_ws(' ',t1.feature,coalesce(t2.feature,''),coalesce(t3.feature,''),coalesce(t4.feature,''))
as feature
from
wlcredit.t_credit_record_cate1_feature_online t1
left JOIN
wlcredit.t_credit_record_cate2_feature_online t2
ON
t1.tel_index = t2.tel_index
left JOIN
wlcredit.t_credit_record_cate3_feature_online t3
ON
t1.tel_index = t3.tel_index
left JOIN
wlcredit.t_credit_record_cate4_feature_online t4
ON
t1.tel_index = t4.tel_index
;


