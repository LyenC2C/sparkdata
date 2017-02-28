
-- 刘婷  微博信息 芝麻信用

SELECT
t1.tel ,zhima_score,
t2.gender, followers_count, friends_count, statuses_count, favourites_count, created_at, bi_followers_count
from
(
SELECT
zhima_score,snwb,t2.tel
from wlcredit.t_base_user_credit_flag t1 join
wlrefer.t_zlj_uid_name t2 on t1.tel =t2.tel
group by zhima_score,snwb,t2.tel
)t1 join t_base_weibo_user_new t2 on t1.snwb=t2.id
group by t1.tel ,zhima_score,
t2.gender, followers_count, friends_count, statuses_count, favourites_count, created_at, bi_followers_count


-- 异常关键词

SELECT
t1.* ,t2.fraud_sum
from wlcredit.t_base_user_credit_flag t1
 join
(
SELECT  user_id ,sum(size (split(keywords,'\\|'))) as fraud_sum from
wlfinance.t_hx_taobao_fraud_record_tbid_20170105bak_tel
group by user_id
)t2 on t1.user_id=t2.user_id ;

-- 相关性分析
SELECT
corr(  cast(zhima_score as int),cast(buycnt as int))  ,
corr(  cast(zhima_score as int),cast(split(regtime,'.')[0] as int))
from wlcredit.t_base_user_credit_flag t1
left join
t_base_user_profile  t2 on t1.user_id=t2.tb_id ;
