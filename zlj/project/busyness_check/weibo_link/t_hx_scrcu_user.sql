-- 招商银行 微博失联修复
Drop table wlfinance.t_hx_scrcu_user_link_fix;
CREATE TABLE wlfinance.t_hx_scrcu_user_link_fix AS
  SELECT
     t1.weibo_id,
    weibo_id_tel,
    weibo_colleges,
    weibo_company ,
    follow_tels,
    follow_ids
    from
  (
  SELECT
 weibo_id,
    weibo_id_tel,
  concat_ws('^',collect_set( concat_ws(':',follow_id,  follow_id_tel ))) as follow_ids ,
  concat_ws('^',collect_set( follow_id_tel )) as follow_tels
  from
  (
  SELECT
    weibo_id,
    weibo_id_tel,
    follow_id,
    uid AS follow_id_tel
  FROM
    (
      SELECT
        weibo_id,
        weibo_id_tel,
        follow_id
      FROM
        (
          SELECT
            /*+ mapjoin(t1)*/
            t2.weibo_id,
            tel AS weibo_id_tel,
            t2.follow_ids
          FROM
            (
                SELECT
                  phone_no tel,
                  id1 AS weibo_id
                FROM
                  -- 输入手机号
                  wlfinance.t_hx_scrcu_user t1
                  JOIN
                  t_base_uid_tmp t2 ON t1.phone_no = t2.uid AND t2.ds = 'wid'
              group by phone_no ,id1

            ) t1 JOIN
            t_base_weibo_user_fri_bi_friends_groupby_12 t2 ON t1.weibo_id = t2.weibo_id
        ) t LATERAL VIEW explode(split(follow_ids, ',')) tt AS follow_id

    ) t3
    JOIN t_base_uid_tmp t4 ON t3.follow_id = t4.id1 AND t4.ds = 'wid'
--     去重 、聚合
  group by   weibo_id,     weibo_id_tel,     follow_id,uid
  )t  group by weibo_id  ,weibo_id_tel

  )t1 join t_zlj_visual_weibo_baseinfo t2 on t1.weibo_id=t2.weibo_id
;
