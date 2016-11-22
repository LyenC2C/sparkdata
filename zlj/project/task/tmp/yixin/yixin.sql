

--  电商购物

select t1.tel ,t2.city,t1.dsn  from
(
SELECT
SUBSTRING (tel,0,7) as tel_prefix,*
from wlfinance.t_hx_taobao_fraud_record_out
where dsn>20160701
)t1 join  t_base_mobile_loc t2 on t1.tel_prefix=t2.prefix and
t2.city in ('上海','武汉');


-- 关注


SELECT  tel ,t2.city ,finance_weibonames
from
(
SELECT  t1.* ,t2.tel ,SUBSTRING (t2.tel,0,7) as tel_prefix
from t_zlj_api_weibo_fraud t1 join

t_zlj_uid_name t2 on t1.weibo_id =t2.snwb and LENGTH(finance_weibonames)>1
)t1  join t_base_mobile_loc t2 on t1.tel_prefix=t2.prefix and
t2.city in ('上海','武汉');


-- 多平台
SELECT
t1.tel ,t1.hit ,city
from
(
SELECT
split(uid, '\\s+')[0] as tel ,split(uid, '\\s+')[1] as hit  ,SUBSTRING (split(uid, '\\s+')[0] ,0,7) as tel_prefix
from  t_base_uid  where ds='wu_0413'
)t1 join t_base_mobile_loc t2 on t1.tel_prefix=t2.prefix and
t2.city in ('上海','武汉');