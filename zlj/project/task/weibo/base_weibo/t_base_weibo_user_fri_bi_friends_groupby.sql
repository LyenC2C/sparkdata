
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

