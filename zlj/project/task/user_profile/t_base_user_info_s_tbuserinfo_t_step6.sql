-- 加入闲鱼地址信息

DROP TABLE t_base_user_info_s_tbuserinfo_t_step6;

CREATE TABLE t_base_user_info_s_tbuserinfo_t_step6 AS
  SELECT
    t4.*,
    t3.weiboid         AS weibo_id,
    t3.screen_name     AS weibo_screen_name,
    t3.gender          AS weibo_gender,
    t3.followers_count AS weibo_followers_count,
    t3.friends_count   AS weibo_friends_count,
    t3.statuses_count  AS weibo_statuses_count,
    t3.created_at      AS weibo_created_at,
    t3.location        AS weibo_location,
    t3.verified        AS weibo_verified
  FROM
    (
      SELECT
        id AS weiboid,
        screen_name,
        gender,
        followers_count,
        friends_count,
        statuses_count,
        created_at,
        location,
        verified,
        tb_id
      FROM
        t_base_weibo_user t1 JOIN
        (
          SELECT
            t1.id1 AS weiboid,
            t2.id1 AS tb_id
          from
          t_base_uid_tmp t1 JOIN t_base_uid_tmp t2 ON t1.ds='wid' AND t2.ds='ttinfo' AND t1.uid=t2.uid
        ) t2 ON t1.id = t2.weiboid
    ) t3
    RIGHT
    JOIN
  t_base_user_info_s_tbuserinfo_t_step5 t4

ON t3.tb_id=t4.tb_id;




