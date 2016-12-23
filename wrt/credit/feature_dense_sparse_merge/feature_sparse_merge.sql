--稀疏表合并
drop table wlcredit.t_wrt_credit_record_cate_feature;
create table wlcredit.t_wrt_credit_record_cate_feature as
select t1.tel_index,
case when t2.tel_index is null then t1.feature
else concat(t1.feature,' ',t2.feature)
end as feature
from
wlcredit.t_credit_record_cate1_feature t1
left JOIN
wlcredit.t_credit_record_cate2_feature t2
ON
t1.tel_index = t2.tel_index;