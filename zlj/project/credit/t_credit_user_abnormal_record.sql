

CREATE  TABLE  wlcredit.t_credit_user_abnormal_record as
select t2.uid, t1.*
 from
(
SELECT
user_id,title ,dsn
FROM
t_base_ec_record_dev_new where
 (title like '%PS%证件照%'  or title like '%证件照%换底%' or title like '%抠图%' )
or ((title like '%代缴%' or title like '%综合缴费%' or title like '%代支付%') and title not like '%宽带%' and sku not like '%电信%' and sku not like '%移动%' and sku not like '%联通%')
or (title like '%社保%' or title like '%公积金%' or title like '%挂靠%')
or (title like '%苹果%解锁%' and root_cat_name like '%本地化生活服务%')
or ((title like '%安卓%解锁%' or title like '%苹果%解锁%' ) and root_cat_name like '%本地化生活服务%')
or (title like '%银行%流水%' )
and ds='true'

)t1  join t_base_uid_tmp    t2 on t1.user_id=t2.id1 and t2.ds='ttinfo';



create table wlcredit.t_credit_user_abnormal_records as

 select user_id,title ,dsn  from  wlcredit.t_credit_user_abnormal_record;