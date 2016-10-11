



Insert  /user/zlj/tmp/t_base_weibo_user_fri_tel_table ;

-- 展开数据
t_base_weibo_user_fri
-- 互相关注人数
t_base_weibo_user_fri_bi_friends






create table t_zlj_base_weibo_user_fri_followid_tel like t_base_weibo_user_fri;

LOAD DATA  INPATH '/user/zlj/tmp/t_base_weibo_user_fri_tel_table' OVERWRITE INTO TABLE
t_zlj_base_weibo_user_fri_followid_tel PARTITION (ds='20161010')


-- 关联pagerank
create table t_zlj_visul_weibo_link_pagerank as
SELECT
id ,weibo_pagerank  ,follow_id ,pagerank follow_pagerank
from
t_zlj_weibo_pagerank_tel   t1
join

(
SELECT  id ,weibo_pagerank ,follow_id
from
(
SELECT  id ,pagerank weibo_pagerank , ids  from t_zlj_base_weibo_user_fri_followid_tel t1
join t_zlj_weibo_pagerank_tel t2 on t1.id =t2.uid
 )t
 lateral view explode(split(ids,',')) tt as follow_id
)   t2 on  t1.uid=t2.follow_id ;

-- 过滤掉followid 不再主id里面
CREATE TABLE t_zlj_visul_weibo_link_pagerank_filter AS
 SELECT
  t1.id AS                  weibo_id,
  round(weibo_pagerank, 2)  weibo_pagerank,
  follow_id,
  round(follow_pagerank, 2) follow_pagerank
 FROM t_zlj_visul_weibo_link_pagerank t1
  JOIN
  (
   SELECT id
   FROM t_zlj_visul_weibo_link_pagerank
   GROUP BY id
  ) t2 ON t1.follow_id = t2.id
;

-- 聚合followid, 产出结果  65814855



create table t_zlj_visul_weibo_link_pagerank_filter_rs as
SELECT
t1.weibo_id, weibo_pagerank,
t2.profile_image_url,
t2.screen_name,
t2.location ,
t3.tags  ,
 follow_ids
from
(
SELECT
 weibo_id ,weibo_pagerank ,concat_ws(' ',collect_set( follow_id) ) as follow_ids
 from t_zlj_visul_weibo_link_pagerank_filter
 group  by weibo_id ,weibo_pagerank
 )t1
 join t_base_weibo_user t2 on t1.weibo_id=t2.idstr
 left join t_base_weibo_usertag on t1.weibo_id=t3.id
 ;



create table t_zlj_visul_weibo_link_pagerank_follows as

select weibo_id ,weibo_pagerank ,
concat_ws(' ',collect_set( follow_id) ) as follows
from
(
SELECT
id as weibo_id , round(weibo_pagerank,2) weibo_pagerank ,follow_id ,round(follow_pagerank,2)  follow_pagerank
from  t_zlj_visul_weibo_link_pagerank
)t group by weibo_id ,weibo_pagerank ;