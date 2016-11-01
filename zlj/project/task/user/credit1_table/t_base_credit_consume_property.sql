

--  消费特征

Drop table  if EXISTS   t_base_credit_consume_property ;


CREATE  TABLE  t_base_credit_consume_property as
SELECT

t1.user_id ,
score_normal as ac_score_normal,
sum_level ,
avg_month_buycnt
FROM
t_zlj_credit_user_level_info t1  join
t_zlj_credit_user_ac_score_normal t2 on t1.user_id=t2.user_id
;



SELECT
sum_level,case  when cast( split(sum_level,'el')[1] as int )>0 then cast( split(sum_level,'el')[1] as int ) else 0 end    as sum_level
FROM  t_base_credit_consume_property limit 10;