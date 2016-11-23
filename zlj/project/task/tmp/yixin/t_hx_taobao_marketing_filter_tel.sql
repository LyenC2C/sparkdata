
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


-- 淘宝异常+多平台借贷
Drop table  wlfinance.t_hx_taobao_fraud_userinfo_loc ;
create table wlfinance.t_hx_taobao_fraud_userinfo_loc as
SELECT
tel,user_id ,cnt_total,cnt_2016,tb_id ,t2.tel_loc
from
(
	SELECT
t1.* ,t2.id1 as tb_id
from

wlfinance.t_hx_taobao_fraud_record_out t1 join
t_base_uid_tmp  t2  on t2.ds='ttinfo' and t1.tel=t2.uid  and t1.cnt_total>0

)t1 join (
select tb_id,tel_loc  from  t_base_user_profile
where tel_loc like '%上海%' or tel_loc like '%武汉%'
)t2
 on t1.tb_id =t2.tb_id
group by tel,user_id ,cnt_total,cnt_2016,t1.tb_id,tel_loc
;


SELECT
t3.uid
from
(
select t1.* ,t2.uid,SUBSTRING (t2.uid,0,7) as tel_prefix
from
(
SELECT weibo_id , desc_fraud_score+nick_fraud_score as score
from t_zlj_api_weibo_fraud
where (desc_fraud_score+nick_fraud_score)>1
)t1 join t_base_uid_tmp  t2  on t2.ds='wid' and t1.weibo_id=t2.id1
)t3 join t_base_mobile_loc t4 on t3.tel_prefix=t4.prefix and t4.city in ('上海','武汉')
;