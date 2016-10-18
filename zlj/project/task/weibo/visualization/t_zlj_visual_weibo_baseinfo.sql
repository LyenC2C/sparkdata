

-- 418363499

DROP  table  t_zlj_visual_weibo_baseinfo ;
create table  t_zlj_visual_weibo_baseinfo as
select

t1.id as weibo_id ,t1.screen_name,
case when  LENGTH(t4.birthday)>6 and 2016 -CAST (split(t4.birthday,'-')[0] as int ) >10 and 2016 -CAST (split(t4.birthday,'-')[0] as int )<75
then t4.birthday else NULL end  as birthday ,
t1.gender ,t1.location,
weibo_colleges,
weibo_company

from

t_base_weibo_user t1
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

weibo_id                bigint            微博id
screen_name             string            微博昵称
birthday                string            出生年月 这个字段数据稀少，星座从里面提取
gender                  string            性别 m女 f男
location                string            地域
weibo_colleges          string            学校 可能有多个学校，用|线隔开，最高学历取起一个
weibo_company           string            公司  多个，用|隔开，最近一个公司取第一个