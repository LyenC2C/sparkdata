


create table t_base_credit_user_property as
SELECT
tb_id ,
tage,
tgender,
'*' as conste ,
'*' as blood_type
FROM
t_base_user_info_s_tbuserinfo_t
where tb_id is not null
;

