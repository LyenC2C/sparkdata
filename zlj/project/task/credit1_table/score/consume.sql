

select

alipay*10,
log10(regtime_month)*0.5,
log10(buycnt)*2,

FROM

t_base_credit_consume_basic_property t1
join t_base_credit_consume_property t2 on t1.tb_id=t2.tb_id

join t_base_credit_consume_perfer t3