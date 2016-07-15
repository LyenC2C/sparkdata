DROP TABLE  t_base_credit_consume_join_data;

CREATE TABLE  t_base_credit_consume_join_data as

SELECT
t1.tb_id ,
alipay       ,
regtime_month,
buycnt       ,
verify_level ,
 size(split(dim,'\\|')) as dim_size           ,
-- esp_perfer    ,
size(split(brand,'\\|'))  as brand_size  ,
case when cast( split(user_per_level,'_')[1] as int )>0 then cast( split(user_per_level,'_')[1] as int ) else 0 end    as user_per_level ,
ac_score_normal ,
case when cast( split(sum_level,'el')[1] as int )>0 then cast( split(sum_level,'el')[1] as int ) else 0 end    as sum_level ,
avg_month_buycnt

 from

t_base_credit_consume_basic_property t1

left join t_base_credit_consume_perfer t2 on t1.tb_id=t2.tb_id

left join t_base_credit_consume_property t3 on t1.tb_id =t3.user_id
where  length(sum_level)>0 ;