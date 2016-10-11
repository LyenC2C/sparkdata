



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


-- 聚合followid, 产出结果  65814855
create table t_zlj_visul_weibo_link_pagerank_follows as

select weibo_id ,weibo_pagerank ,
concat_ws(' ',collect_set(concat_ws('-',follow_id,CAST(follow_pagerank as string) ))) as follows
from
(
SELECT
id as weibo_id , round(weibo_pagerank,2) weibo_pagerank ,follow_id ,round(follow_pagerank,2)  follow_pagerank
from  t_zlj_visul_weibo_link_pagerank
)t group by weibo_id ,weibo_pagerank ;