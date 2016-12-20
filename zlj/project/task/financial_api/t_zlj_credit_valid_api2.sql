
-- 近3月购买
create table  t_zlj_credit_valid_api2_step1  AS

SELECT  user_id ,COUNT(1) as shop_num
FROM
 t_base_ec_record_dev_new_simple
where dsn>20160701  group by user_id ;


--

Drop table t_zlj_credit_valid_api2_step2 ;
create table t_zlj_credit_valid_api2_step2 as
select
t2.id1 as tb_id , decrypted_tel,tie_num
from
(
SELECT
decrypted_tel ,COUNT (1) as tie_num
FROM t_base_credit_58_info  where
decrypted_tel  rlike   '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}' AND
  LENGTH(decrypted_tel)=11 and  ds='20161009'
  GROUP by decrypted_tel
  )t1 join t_base_uid_tmp t2 on t2.ds='ttinfo' and t1.decrypted_tel =t2.uid
  ;


-- 数据关联
drop table wlbase_dev.t_zlj_credit_valid_api2_step3;
create table wlbase_dev.t_zlj_credit_valid_api2_step3  as
SELECT
t1.* ,
t2.shop_num ,
t3.tie_num
from
(
SELECT  t1.*,
  regtime as tb_regtime ,
  tb_nick,
  weibo_created_at  as weibo_regtime ,
  weibo_screen_name  as weibo_nick_name,
  weibo_statuses_count ,
  weibo_friends_count ,
  weibo_followers_count,
  xianyu_birthday as xianyu_info ,
  qq_name,
  58_tel ,
  58_nickname
from t_zlj_credit_valid_api1_step1  t1 join t_base_user_profile t2 on
  t1.tb_id=t2.tb_id
  ) t1 left join t_zlj_credit_valid_api2_step1 t2 on t1.tb_id=t2.user_id
   left join t_zlj_credit_valid_api2_step2 t3  on t1.tb_id=t3.tb_id
  ;



DROP TABLE  t_zlj_credit_valid_api2 ;
create table t_zlj_credit_valid_api2 as
  SELECT
    tb_id,
    case when tb_id  is not null and tb_id<>'-' then 1 else -1 end  as tb_flag ,
    tb_nick,
    tb_regtime ,
    case when xianyu_info  is not null and xianyu_info<>'-' then 1 else -1 end  as tb_xianyu_flag ,
    case when weibo_id  is not null and weibo_id<>'-' then 1 else -1 end  as snwb_id_flag ,
    weibo_nick_name ,
    weibo_regtime ,

    case when qqid  is not null and qqid<>'-' then 1 else -1 end  as qq_flag,
    qqid ,
    case when email  is not null and email<>'-' then 1 else -1 end  as email_flag ,
    email,
    case when 58_tel  is not null and 58_tel<>'-' then 1 else -1 end  as 58_flag,
    58_nickname ,
    shop_num ,
    tie_num
    from
    t_zlj_credit_valid_api2_step3
    ;
--


--
-- SELECT
--
-- COUNT (1)
-- from
--   t_zlj_tmp_weiboid_5w t1 join t_zlj_visul_tel_bi_friends_link_rs t2 on t1.weibo_id =t2.weibo_id ;
