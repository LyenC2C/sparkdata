

select

alipay*10,
log10(regtime_month)*0.5,
log10(buycnt)*2,
(verify_level+1)/6*6,
user_per_level*1 ,
ac_score_normal*2,
sum_level*3,
avg_month_buycnt*1

FROM

t_base_credit_consume_basic_property t1
join t_base_credit_consume_property t2 on t1.tb_id=t2.tb_id

join t_base_credit_consume_perfer t3 ;