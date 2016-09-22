-- 打通手机号用户的关注数据


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


-- 拿出四川用户数据

DROP TABLE t_zlj_weibo_fri_tel_sichuan;
CREATE TABLE t_zlj_weibo_fri_tel_sichuan AS
  SELECT
    /*+ mapjoin(t1)*/ t2.*
  FROM
    (SELECT id
     FROM t_base_weibo_user
     WHERE ds = 20160829 AND location LIKE '%成都%') t1
    JOIN t_zlj_weibo_fri_tel t2 ON t1.id = t2.id;


CREATE TABLE t_zlj_weibo_fri_tel_sichuan_lateral AS
  SELECT concat_ws('\t', CAST(t1.id AS string), t1.fid) AS f
  FROM
    (
      SELECT
        id,
        fid
      FROM t_zlj_weibo_fri_tel_sichuan lateral VIEW explode(split(ids, ',')) tt AS fid
    ) t1
    JOIN
    (
      SELECT id1 AS weibo_id
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
    ) t2 ON t1.fid = t2.weibo_id;

