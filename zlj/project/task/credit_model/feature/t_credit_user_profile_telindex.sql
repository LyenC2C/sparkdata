

-- 用户基本信息特征


SELECT
FROM

t_base_user_profile_telindex



地域特征
微博粉丝特征

购买/年限月份  频次特征

  drop table wlcredit.t_credit_user_profile_feature;
  create table wlcredit.t_credit_user_profile_feature as
  SELECT
  tel_index ,
  case when alipay like '已通过支付宝实名认证' then 1 else 0 end  as alipay_flag  ,
  buycnt  ,
  cast(regexp_replace(verify, 'VIP等级', '')  as int) as  verify_level ,
  -- verify,
  (12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-')))) regtime_month ,
  buycnt/((12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-'))))+1) as buy_freq,
  split(tel_loc,'\\s+')[0] as prov,
  model_predict_gender
  from
  t_base_user_profile_telindex ;