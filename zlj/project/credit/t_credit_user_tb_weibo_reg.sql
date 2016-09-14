



create table wlcredit.t_credit_user_tb_weibo_register as
select
t2.tb_id,t2.regtime as tb_regtime ,weibo_id ,weibo_created_at

 from
t_base_uid_tmp  t1
join
t_base_user_info_s_tbuserinfo_t_step6  t2 on t1.id1=t2.tb_id and t1.ds='ttinfo'  ;

create table
wlcredit.t_credit_user_tb_weibo_register
as
SELECT
tb_id,tb_regtime,weibo_id ,weibo_created_at
from t_credit_user_tb_weibo_reg ;