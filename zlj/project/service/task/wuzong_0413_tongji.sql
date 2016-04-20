


create table t_zlj_task_wu_tongji as

SELECT
t2.* ,t1.id1,id2

from
t_base_uid t1

join

t_base_user_buy_info t2
on t1.ds='0413' and t1.uid=t2.user_id ;


select * from t_base_uid where  ds='wu_04130' limit 10;


select  user_id ,id1,id2, tgender,tage,tloc from t_zlj_task_wu_tongji group by user_id,tgender,tage,tloc,id1,di2 ;

select id2,root_cat_name,count(1) from t_zlj_task_wu_tongji group by root_cat_name,id2;


select  t2.id1,id2, ulevel  from  t_zlj_perfer_user_level_mult t1 join t_base_uid t2 on t1.uid=t2.uid and t2.ds='0413'



select count(1) from t_qqweibo_user_info ;

 SELECT  COUNT (DISTINCT  word )from
 (
select word  from t_qqweibo_user_info
 LATERAL  VIEW explode(split(tags_r, '_'))t1  AS word
)t
;



