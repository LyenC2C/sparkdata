create table  t_zlj_visual_weibo_baseinfo as
select

t1.id as weibo_id ,t1.screen_name,
case when  LENGTH(t4.birthday)>6 and 2016 -CAST (split(t4.birthday,'-')[0] as int ) >10 and 2016 -CAST (split(t4.birthday,'-')[0] as int )<75
then t4.birthday else '' end  as birthday ,
t1.gender ,t1.location,
weibo_colleges,
weibo_company

from

t_base_weibo_user t1

left join t_base_weibo_user_basic t4  on t1.id =t4.id
LEFT join
 (
 SELECT id as weibo_id ,concat_ws('|',collect_set(college )) weibo_colleges from t_base_weiboid_college group by id
  ) t2 on t1.id=t2.weibo_id
LEFT join
 (
 SELECT id as weibo_id ,concat_ws('|',collect_set(concat_ws('_', company,department ) )) as weibo_company  from
 t_base_weiboid_career group by id
  )t3 on t1.id=t3.weibo_id
  ;