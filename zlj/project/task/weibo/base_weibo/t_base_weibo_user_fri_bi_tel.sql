

-- 100603551
-- 打通手机号相互关注的关系数据

Drop table  t_base_weibo_user_fri_bi_tel;
CREATE TABLE t_base_weibo_user_fri_bi_tel AS

  SELECT
    /*+ mapjoin(t1)*/
    t2.weibo_id,
    t2.follow_ids
  FROM
    (
      SELECT id1 AS weibo_id
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
    ) t1 JOIN
    t_base_weibo_user_fri_bi_friends_groupby t2 ON t1.weibo_id = t2.weibo_id ;



--
Drop table  t_base_weibo_user_fri_bi_tel_null_test;
CREATE TABLE t_base_weibo_user_fri_bi_tel_null_test AS

  SELECT
    /*+ mapjoin(t1)*/
   t1.weibo_id ,uid
  FROM
    (
      SELECT id1 AS weibo_id,uid
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
    ) t1 left JOIN
    t_base_weibo_user_fri_bi_friends_groupby t2 ON t1.weibo_id = t2.weibo_id  where follow_ids is null;
--

-- 没有互相关注用户的打通手机号 用户基本信息
create table t_base_weibo_user_fri_bi_tel_null_test_userinfo as
SELECT t1.uid ,t2.* from t_base_weibo_user_fri_bi_tel_null_test t1 join
t_base_weibo_user t2 on t1.weibo_id=t2.id
 ;

SELECT
-- check

SELECT * from  t_base_weibo_user_fri_bi_tel where weibo_id=2071271691 ;

