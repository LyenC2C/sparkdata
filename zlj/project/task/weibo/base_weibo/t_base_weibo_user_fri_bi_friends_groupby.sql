
-- 相互关注的人聚合

create table t_base_weibo_user_fri_bi_friends_groupby as
  SELECT  id1 as weibo_id ,
  concat_ws(',',collect_set( id2 )) as follow_ids
  from
  (
    SELECT
      id1,
      id2
    FROM t_base_weibo_user_fri_bi_friends
    UNION ALL
    SELECT
      id2 AS id1,
      id1 AS id2
    FROM t_base_weibo_user_fri_bi_friends
  )t group by id1
;


create table t_base_weibo_user_fri_bi_friends_groupby_12 as
  SELECT  id1 as weibo_id ,
  concat_ws(',',collect_set( id2 )) as follow_ids
  from
  (
    SELECT
      id1,
      id2
    FROM t_base_weibo_user_fri_bi_friends_v12
    UNION ALL
    SELECT
      id2 AS id1,
      id1 AS id2
    FROM t_base_weibo_user_fri_bi_friends_v12
  )t group by id1
;


SELECT COUNT(1) from t_base_weibo_user_fri_bi_friends_groupby where size(split(follow_ids,','))>5000

-- check

SELECT * from  t_base_weibo_user_fri_bi_friends_groupby where weibo_id=2071271691 ;

SELECT * from  t_zlj_visual_weibo_baseinfo where weibo_id=2071271691 ;