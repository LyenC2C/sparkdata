CREATE  TABLE  if not exists t_zlj_dc_weibodata (
mid  string    COMMENT '',
orgin_name bigint COMMENT '原始微博用户' ,
orgin_id string ,
user_name string ,
user_id string ,
next_user_name string ,
time_cut bigint ,
content  string
)
COMMENT 'DC 比赛数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


SELECT mid,
case when next_user_name is null then  orgin_name ,
case when next_user_name is null then  orgin_id ,
user_name ,
user_id
from t_zlj_dc_weibodata_3w
 ;




SELECT

COUNT(1)
FROM
(
select  orgin_name , orgin_id
from
(
SELECT  orgin_name , orgin_id  from t_zlj_dc_weibodata
UNION  ALL
SELECT user_name  orgin_name , user_id  orgin_id  from t_zlj_dc_weibodata

)t group by orgin_name , orgin_id

)t1 RIGHT join

 (select next_user_name from t_zlj_dc_weibodata  where LENGTH(next_user_name)>2 )t2 on t1.orgin_name=t2.next_user_name

  where t1.orgin_name is null ;



SELECT

COUNT(1)
FROM
(
select  orgin_name , orgin_id
from
(
SELECT  orgin_name , orgin_id  from t_zlj_dc_weibodata
UNION  ALL
SELECT user_name  orgin_name , user_id  orgin_id  from t_zlj_dc_weibodata

)t group by orgin_name , orgin_id

)t1 RIGHT join

 (select next_user_name from t_zlj_dc_weibodata  where LENGTH(next_user_name)>2 )t2 on t1.orgin_name=t2.next_user_name

  where t1.orgin_name is null ;






SELECT count(1)  FROM t_zlj_dc_weibodata_orgin_name_orgin_id where length(orgin_name)>1 ;

create table t_zlj_dc_weibodata_orgin_name_orgin_id
 as
select  orgin_name , orgin_id
from
(
SELECT  orgin_name , orgin_id  from t_zlj_dc_weibodata
UNION  ALL
SELECT user_name  orgin_name , user_id  orgin_id  from t_zlj_dc_weibodata

)t group by orgin_name , orgin_id ;



DROP  table t_zlj_dc_weibodata_next_user_name_id ;
create table  t_zlj_dc_weibodata_next_user_name_id as
SELECT
t2.*, t1.orgin_id as next_user_name_id
FROM
(
  SELECT *
  FROM t_zlj_dc_weibodata_orgin_name_orgin_id  where length(orgin_name)>1
)t1 join

 (select * from t_zlj_dc_weibodata_3w where length(next_user_name)>1 and length(next_user_name)<20  )t2 on t1.orgin_name=t2.next_user_name

union all
 select * , NULL as next_user_name_id  from t_zlj_dc_weibodata_3w where length(next_user_name)<=1  ;



DROP   TABLE t_zlj_dc_weibodata_3w_rs;

create table t_zlj_dc_weibodata_3w_rs as
SELECT mid,
  t3.origin_id ,
  t4.rn as user_id ,time_cut, content
FROM

(
SELECT mid,
  t2.rn as origin_id ,
  user_id ,time_cut, content
FROM
(
SELECT
          mid,
          CASE WHEN next_user_name_id IS NOT NULL
            THEN next_user_name_id
          ELSE orgin_id end  AS orgin_id,
          user_id,
          time_cut, content

  from
t_zlj_dc_weibodata_next_user_name_id
)t1 join t_zlj_dc_weibodata_3w_username_id t2  on t1.orgin_id=t2.orgin_id
)t3 join t_zlj_dc_weibodata_3w_username_id t4 on t3.user_id =t4.orgin_id ;

-- SELECT count(1)
-- FROM
-- (
--   SELECT *
--   FROM t_zlj_dc_weibodata_orgin_name_orgin_id  where length(orgin_name)>1
-- )t1 join
--
--  (select * from t_zlj_dc_weibodata_3w where length(next_user_name)>1 and length(next_user_name)<20  )t2 on t1.orgin_name=t2.next_user_name
--

SELECT

  mid,
  t2.rn AS orgin_id,
  t4.rn AS user_id,
  time_cut,
  content
FROM
  (
    SELECT
      mid,
      t2.rn AS orgin_id,
      user_id,
      time_cut,
      content
      (
          SELECT
          mid,
          CASE WHEN next_user_name_id IS NOT NULL
            THEN next_user_name_id
          ELSE orgin_id AS orgin_id,
          user_id,
          time_cut, content
          FROM
          t_zlj_dc_weibodata_next_user_name_id
      )        t1 JOIN t_zlj_dc_weibodata_3w_username_id t2 ON t1.orgin_id=t2.orgin_id
  ) t3 JOIN t_zlj_dc_weibodata_3w_username_id t4 ON t3.user_id = t4.orgin_id ;


-- 提取三万用户数据
DROP  TABLE t_zlj_dc_weibodata_3w ;
CREATE  TABLE  t_zlj_dc_weibodata_3w as
SELECT  t2.*
from
(
SELECT  mid from
(
  SELECT  mid ,num ,  ROW_NUMBER() OVER (PARTITION BY id ORDER BY num  DESC) AS rn

from
(
  SELECT mid ,COUNT(1) as num,1 as id
  from t_zlj_dc_weibodata group by mid
  )t0

  )tn where rn>10000 and rn<40000
)t1 join t_zlj_dc_weibodata t2 on  t1.mid=t2.mid

where
CAST(orgin_id as bigint) >0
and CAST(user_id as bigint) >0
 ;


--


DROP  TABLE t_zlj_dc_weibodata_3w_username_id ;
  create TABLE t_zlj_dc_weibodata_3w_username_id as

select  orgin_id , ROW_NUMBER() OVER (PARTITION BY id ORDER BY orgin_id  DESC) AS rn
from
(
SELECT   CAST(orgin_id as bigint) orgin_id,1 as id
FROM
(
SELECT  orgin_name , orgin_id  from t_zlj_dc_weibodata_3w
UNION  ALL
SELECT user_name  orgin_name , user_id  orgin_id  from t_zlj_dc_weibodata_3w
)t1

group by  orgin_id

)t2 ;