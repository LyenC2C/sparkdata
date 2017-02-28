

-- 芝麻分标准集
drop table wlcredit.t_base_user_credit_flags ;
 create table wlcredit.t_base_user_credit_flags as
SELECT
t2.uid as tel ,t1.*
from
(
select user_id ,tel_index,zhima_score,label from
(
  select tel as user_id ,id1 as tel_index ,id2 as zhima_score ,1 as label  from  wlfinance.t_zlj_base_match
  where ds='zhima'  and  id2 >=600 and rand()>0.85
  union all
  select tel as user_id ,id1 as tel_index ,id2 as zhima_score ,0 as label
  from  wlfinance.t_zlj_base_match where ds='zhima'   and  id2<600 and id2>=500 and rand()<0.6
union all
select tel as user_id ,id1 as tel_index ,id2 as zhima_score ,0 as label
  from  wlfinance.t_zlj_base_match where ds='zhima'   and  id2<500
   )t group by user_id ,tel_index,zhima_score,label
   )

   t1 join wlrefer.t_zlj_phone_rank_index t2 on t1.user_id=t2.tb_id;


create table wlcredit.t_base_user_credit_flags as
SELECT t1.user_id,t2.tel_index as tel_index, t1.zhima_score ,label,uid as tel
from wlcredit.t_base_user_credit_flag t1
join wlrefer.t_zlj_phone_rank_index t2 on t1.user_id=t2.tb_id
group by t1.user_id,t2.tel_index ,zhima_score ,uid ,label
;



-- 测试样本5000条
select *
 from  wlcredit.t_base_user_credit_flag  where   zhima_score>=600 and
  length( regexp_extract(tel,'^134|^135|^136|^137|^138|^139|^147|^150|^151|^152|^157|^158|^159|^182|^183|^184|^187|^188|^178',0))>0
  limit 3000
  union all
  select *
  from  wlcredit.t_base_user_credit_flag  where   zhima_score<600
   and length( regexp_extract(tel,'^134|^135|^136|^137|^138|^139|^147|^150|^151|^152|^157|^158|^159|^182|^183|^184|^187|^188|^178',0))>0
    limit 1000
union all
select *
  from  wlcredit.t_base_user_credit_flag where    zhima_score<500  and
  length( regexp_extract(tel,'^134|^135|^136|^137|^138|^139|^147|^150|^151|^152|^157|^158|^159|^182|^183|^184|^187|^188|^178',0))>0
  limit 1000



197747  60915   149609  164634
   SELECT
   count (DISTINCT t1.tel) ,
   count (DISTINCT qq ) as qq_count,
   count (DISTINCT snwb)  as qq_count1,
   count (DISTINCT email)  as qq_count12
   from wlcredit.t_base_user_credit_flag t1 join wlrefer.t_zlj_uid_name t2 on t1.tel =t2.tel
;


-- 微博分析

SELECT
zhima_score, t2.*
from
(
SELECT
zhima_score,snwb
from wlcredit.t_base_user_credit_flag t1 join
wlrefer.t_zlj_uid_name t2 on t1.user_id =t2.tb
)t1 join t_base_weibo_user_new t2 on t1.snwb=t2.id ;



SELECT
tel ,zhima_score, t2.*
from
(
SELECT
zhima_score,snwb,tel
from wlcredit.t_zlj_zhima20170110_userinfo t1 join
wlrefer.t_zlj_uid_name t2 on t1.tb_id =t2.tb
)t1 join t_base_weibo_user_new t2 on t1.snwb=t2.id ;



-- 模型训练标准集  49647
create table wlcredit.t_zlj_zhima_model_train as
select
t2.tel,t2.snwb, qqwb, prov, city, carrier, qq,  t1.*
from
(
select * from wlcredit.t_base_user_credit_flag where rand()>0.75
) t1 join wlrefer.t_zlj_uid_name t2 on t1.user_id=t2.tb ;


-- t_base_user_credit_flag 197851
-- 197890
drop table wlcredit.t_zlj_zhima_model_train_lt ;
 create table wlcredit.t_zlj_zhima_model_train_lt as
select
t2.snwb, qqwb, carrier, qq,email,  t1.*
from
(
select * from wlcredit.t_base_user_credit_flag
  ) t1 left  join wlrefer.t_zlj_uid_name t2 on t1.user_id=t2.tb ;




-- IV
-- SELECT
-- t1.* ,
-- t2.alipay               ,
-- t2.buycnt               ,
-- t2.verify               ,
-- t2.regtime              ,
-- t2.tb_nick              ,
-- t2.tb_location          ,
-- t2.qq_gender            ,
-- t2.qq_age               ,
-- t2.qq_name              ,
-- t2.qq_loc               ,
-- t2.qq_find_schools      ,
-- t2.tel_loc              ,
-- t2.xianyu_gender        ,
-- t2.xianyu_birthday      ,
-- t2.xianyu_constellation ,
-- t2.xianyu_province      ,
-- t2.xianyu_city          ,
-- t2.xianyu_detail_loc    ,
-- t2.model_predict_gender ,
-- t2.weibo_id             ,
-- t2.weibo_screen_name    ,
-- t2.weibo_gender         ,
-- t2.weibo_followers_count,
-- t2.weibo_friends_count  ,
-- t2.weibo_statuses_count ,
-- t2.weibo_created_at     ,
-- t2.weibo_location       ,
-- t2.weibo_verified       ,
-- t2.weibo_colleges       ,
-- t2.weibo_company
-- from wlcredit.t_base_user_credit_flag t1 left join wlbase_dev.t_base_user_profile t2 on t1.user_id=t2.tb_id;
