
-- 消费特征

create TABLE  t_base_credit_consume_basic_property as
SELECT
tb_id,
alipay,
regtime,
buycnt,
verify

FROM
t_base_user_info_s_tbuserinfo_t
where tb_id is not null
;