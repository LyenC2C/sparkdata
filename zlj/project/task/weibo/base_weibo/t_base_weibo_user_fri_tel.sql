

-- 100603551
-- 打通手机号用户的关系数据
alter table t_zlj_weibo_fri_tel rename to  t_base_weibo_user_fri_tel;
CREATE TABLE t_zlj_weibo_fri_tel AS

  SELECT
    /*+ mapjoin(t1)*/
    id,
    ids
  FROM
    (
      SELECT id1 AS weibo_id
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
    ) t1 JOIN
    t_base_weibo_user_fri t2 ON t1.weibo_id = t2.id AND t2.ds = 20160902;



-- check

SELECT * from  t_zlj_weibo_fri_tel where id=2071271691 ;

