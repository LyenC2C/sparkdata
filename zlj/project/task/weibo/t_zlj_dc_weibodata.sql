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



SELECT count(1)
from (
SELECT mid from
t_zlj_dc_weibodata
group by mid

)t

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

COUNT(DISTINCT next_user_name )
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



-- 提取三万用户数据
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

  )tn where rn>30000 and rn<60000
)t1 join t_zlj_dc_weibodata t2 on  t1.mid=t2.mid
 ;


--


DROP  TABLE t_zlj_dc_weibodata_3w_username_id ;
create TABLE t_zlj_dc_weibodata_3w_username_id as
SELECT orgin_name , orgin_id
FROM
(
SELECT  orgin_name , orgin_id  from t_zlj_dc_weibodata_3w
UNION  ALL
SELECT user_name  orgin_name , user_id  orgin_id  from t_zlj_dc_weibodata_3w
UNION  ALL
SELECT next_user_name  orgin_name , NULL  as  orgin_id  from t_zlj_dc_weibodata_3w
)t1
where orgin_name is not null
group by orgin_name , orgin_id ;