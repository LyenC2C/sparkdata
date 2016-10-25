CREATE  table wlfinance.t_hx_taobao_fraud_record2_tel_userid_tel as
SELECT user_id ,uid as tel from
wlfinance.t_hx_taobao_fraud_record2_tel_userid t1 join
t_base_uid_tmp t2 on t1.user_id=t2.id1 and t2.ds='ttinfo' ;