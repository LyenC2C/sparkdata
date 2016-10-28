

-- 418363499
-- step1
-- 关联基本信息与 教育 事业数据

DROP  table  t_zlj_visual_weibo_baseinfo_step1 ;
create table  t_zlj_visual_weibo_baseinfo_step1 as
select
t1.id as weibo_id ,t1.screen_name,
case when  LENGTH(t4.birthday)>6 and 2016 -CAST (split(t4.birthday,'-')[0] as int ) >10 and 2016 -CAST (split(t4.birthday,'-')[0] as int )<75
then t4.birthday else NULL end  as birthday ,
t1.gender ,t1.location,
t1.profile_image_url ,
t1.avatar_large,
t1.avatar_hd ,
weibo_colleges,
weibo_company ,
followers_count  ,
friends_count,
statuses_count ,
t1.verified ,
t1.created_at ,
t1.bi_followers_count
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


-- SELECT id ,friends_count ,followers_count ,log10(followers_count+1)*0.7+log10(friends_count+1)*0.3 from t_zlj_visual_weibo_baseinfo_step1 limit 100;

-- step2
Drop table t_zlj_visual_weibo_baseinfo ;
create table t_zlj_visual_weibo_baseinfo as
SELECT
t1.* ,log10(followers_count+1)*0.7+log10(friends_count+1)*0.3 as  weibo_pagerank ,t3.tags
from  t_zlj_visual_weibo_baseinfo_step1 t1
left join t_zlj_weibo_pagerank_tel t2 on t1.weibo_id=t2.uid
left join (SELECT  cast(id as string) weibo_id  ,tags from t_base_weibo_usertag group by id ,tags ) t3
 on t1.weibo_id =t3.weibo_id
  ;



weibo_id                bigint            微博id
screen_name             string            微博昵称
birthday                string            出生年月 这个字段数据稀少，星座从里面提取
gender                  string            性别 m女 f男
location                string            地域
weibo_colleges          string            学校 可能有多个学校，用|线隔开，最高学历取起一个
weibo_company           string            公司  多个，用|隔开，最近一个公司取第一个

t_zlj_visual_weibo_baseinfo
weibo_id                bigint              微博id
screen_name             string               微博昵称
birthday                string              出生年月 这个字段数据稀少，星座从里面提取
gender                  string              性别 m女 f男
location                string              地域
profile_image_url       string              微博用户头像连接
weibo_colleges          string              学校 可能有多个学校，用|线隔开，最高学历取起一个
weibo_company           string               公司  多个，用|隔开，最近一个公司取第一个
weibo_pagerank          float                微博影响力  0-1的浮点数
tags                    string               微博标签  有空值NULL  774:手机 529:财经 332:数码 758:股票 1405:汽车


t_zlj_visul_weibo_link_pagerank_filter_rs

weibo_id    微博id
follow_ids  微博互相关注用户    1069922975,1206001067,2151818417。 逗号风格

t_base_weibo_user_keywords

user_id   微博id
keywords   关键词+权重