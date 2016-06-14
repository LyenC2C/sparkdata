

-- numRows=68541728


CREATE TABLE t_zlj_credit_children_feed_data AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE content LIKE '%孩子%' or content LIKE '%儿子%'  or content LIKE '%女儿%'  or   content LIKE '%婴儿%';


-- 评论 挖掘用户 27176178
SELECT count(1) from (SELECT user_id from t_zlj_credit_children_feed_data group by user_id)t ;



CREATE TABLE t_zlj_credit_children_feed_record_data AS

SELECT
item_id          ,
user_id          ,
content_length   ,
annoy            ,
ds               ,
datediff         ,
cat_id           ,
root_cat_id      ,
brand_id         ,
bc_type          ,
price
from

t_base_ec_record_dev_new_simple
where root_cat_id  in
(50008165,
50012422,
50015815,
50014812,
50017637,
50017638,
50022517,
35,
25,
122650005)
;

create table t_zlj_credit_children_feed_record_data_usercat_count as
SELECT user_id, concat_ws('\t',collect_set(root_num))
from
(
SELECT
user_id, concat_ws(':',root_cat_id , CAST(COUNT(1) as string) )  as root_num
FROM t_zlj_credit_children_feed_record_data group by user_id,root_cat_id
)t group by user_id ;




create table t_zlj_credit_children_feed_record_data_usercat_score as
SELECT user_id, sum(s)+COUNT(1)*2 as score
from
(
SELECT
user_id, count(1)*0.5 as s
FROM t_zlj_credit_children_feed_record_data group by user_id,root_cat_id
)t group by user_id ;


SELECT  score,COUNT(1)  FROM  t_zlj_credit_children_feed_record_data_usercat_score group by score

-- 84540361
SELECT count(1) from (SELECT user_id from t_zlj_credit_children_feed_record_data group by user_id)t ;


-- 92133930

SELECT count(1) from (

select user_id from (
SELECT user_id from t_zlj_credit_children_feed_data group by user_id
union ALL
SELECT user_id from t_zlj_credit_children_feed_record_data group by user_id

 )t2 group by user_id
 )t ;


 t_wrt_children_keywords_record
create table t_wrt_children_keywords_record AS
select t2.* from
(select user_id from wlbase_dev.t_zlj_credit_children_feed_data group by user_id)t1
join
(select user_id,root_cat_id,root_cat_name,cat_id from wlbase_dev.t_base_ec_record_dev_new)t2
on
t1.user_id = t2.user_id ;

select count(1),root_cat_id,root_cat_name

from t_wrt_children_keywords_record group by root_cat_id,root_cat_name