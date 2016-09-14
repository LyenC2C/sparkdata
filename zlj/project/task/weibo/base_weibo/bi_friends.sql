

--  计算相互关注人数
create table t_base_weibo_user_fri_bi_friends as
SELECT
id1, id2 ,COUNT(1) as num
from
(
select
sort_array(array(id,fid))[0]  as id1 ,sort_array(array(id,fid))[1] as id2
from t_base_weibo_user_fri lateral view explode(split(ids,',')) tt as fid
)t group by id1,id2  HAVING COUNT(1)>1
;




create table t_base_weibo_user_fri_bi_friends_tmp_louvain_testdata as
SELECT id1 ,id2
FROM
  t_base_weibo_user_fri_bi_friends
limit 1000000 ;


INSERT  overwrite table t_base_uid_tmp partition(ds='wid1')
SELECT split(uid,'\\s+')[0] as uid ,split(uid,'\\s+')[1] as id1 ,
  '' as id2 ,
  '' as id3 ,
  '' as id4 ,
  '' as id5 ,
  '' as id6 ,
  '' as id7
  from t_base_uid_tmp where ds='wid' ;




-- 挑出真实tle数据

-- step1 挑出被关注着打通tel
create table t_base_weibo_user_fri_check_18kw_data_step1 as
 SELECT  id ,ids_list
          FROM(select cast(id1 as bigint) id1 from  t_base_uid_tmp where ds='wid')t2 join
            (SELECT  id ,split(ids,',') as ids_list from t_base_weibo_user_fri
            where  size(split(ids,','))>1) t1
              on t1.id =t2.id1
 ;


create table t_base_weibo_user_fri_check_18kw_data_step1 as

  SELECT id ,cast(follow_id as BIGINT) follow_id FROM
        (
          SELECT  id ,ids_list
          FROM(select cast(id1 as bigint) id1 from  t_base_uid_tmp where ds='wid')t2 join
            (SELECT  id ,split(ids,',') as ids_list from t_base_weibo_user_fri where size(split(ids,','))<5000 and size(split(ids,','))>1) t1
              on t1.id =t2.id1
        )t lateral view explode(ids_list) tt as follow_id ;

create table t_base_weibo_user_fri_check_18kw_data as
  SELECT t3.id ,follow_id
  from
    (select cast(id1 as bigint) id1 from  t_base_uid_tmp where ds='wid') t4
     join  (
      SELECT id ,cast(follow_id as BIGINT) follow_id FROM
        (
          SELECT  id ,ids_list
          FROM(select cast(id1 as bigint) id1 from  t_base_uid_tmp where ds='wid')t2 join
            (SELECT  id ,split(ids,',') as ids_list from t_base_weibo_user_fri where size(split(ids,','))<5000 and size(split(ids,','))>1) t1
              on t1.id =t2.id1
        )t lateral view explode(ids_list) tt as follow_id
    )t3  on t3.follow_id =t4.id1 ;


SELECT  count(1) from (SELECT id from t_base_weibo_user_fri group by id)t;


-- 相互关注的id
-- 246816340
SELECT COUNT(1 ) from
(SELECT  *
from
(
SELECT explode(array(id1,id2)) as id
from t_base_weibo_user_fri_bi_friends

)t group by id
)t1  ;


189136874
SELECT count(1) from t_base_uid_tmp where  ds='wid' ;


-- 58537799
-- 有效用户
SELECT COUNT(1 ) from
(SELECT  id
from
(
SELECT explode(array(id1,id2)) as id
from t_base_weibo_user_fri_bi_friends

)t group by id
)t1 join t_base_uid_tmp t2 on t1.id =t2.id1 and t2.ds='wid'
 ;
