
create table wlfinance.t_hx_taobao_marketing_filter as
select * from wlfinance.t_hx_taobao_marketing
where fraud_score>=1 ;


-- 宜信 异常客户 手机号
create table wlfinance.t_hx_taobao_marketing_filter_tel as
SELECT uid, rn , user_id, sum_fraud_score
from
(
select user_id ,sum(fraud_score) as sum_fraud_score
 from wlfinance.t_hx_taobao_marketing_filter group by user_id
 )t1 join t_zlj_phone_rank_index t2 on t1.user_id =t2.tb_id

 ;

 SELECT count(1)
 from
 (select  uid from
 wlfinance.t_hx_taobao_marketing_filter_tel
  group by    uid
)t ;