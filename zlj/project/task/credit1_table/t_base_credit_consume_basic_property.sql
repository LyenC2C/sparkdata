
-- 消费特征

DROP  TABLE  IF EXISTS  t_base_credit_consume_basic_property ;


create TABLE  t_base_credit_consume_basic_property as
SELECT
tb_id,
alipay,
(12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-'))))
as regtime_month,
buycnt,
case when cast( split(verify,'等级')[1] as int )>0 then cast( split(verify,'等级')[1] as int ) else 0 end    as verify_level

FROM
t_base_user_info_s_tbuserinfo_t
where tb_id is not null
;


