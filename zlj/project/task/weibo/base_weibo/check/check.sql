

--  600w  验证
-- 5406805
-- 6418840
SELECT  COUNT(1) from
(
SELECT
predict as weibo_id ,
split(label,':')[1] as  tel
from t_zlj_rong360_rs_ks where ds='phone_weibouser_1027') t1

join (

  SELECT id1 AS weibo_id,uid
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
)t2 on t1.weibo_id=t2.weibo_id and t1.tel=t2.uid  ;



660710
SELECT  COUNT(1)  from
(
SELECT
predict as weibo_id ,
split(label,':')[1] as  tel
from t_zlj_rong360_rs_ks where ds='phone_weibouser_1027'
) t1

join (

  SELECT id1 AS weibo_id,uid
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
)t2 on t1.tel=t2.uid where t1.weibo_id<>t2.weibo_id ;

--
0       660710
1       5406805

SELECT

flag, COUNT(1) from
 (
SELECT  case when  t1.weibo_id=t2.weibo_id  then 1 else 0 end as flag
  from
(
SELECT
predict as weibo_id ,
split(label,':')[1] as  tel
from t_zlj_rong360_rs_ks where ds='phone_weibouser_1027'
) t1

join (

  SELECT id1 AS weibo_id,uid
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
)t2 on t1.tel=t2.uid
)t group by flag
;


-- 8000w lost
DROP table t_zlj_tmp_weibo_100w_check ;
create table t_zlj_tmp_weibo_100w_check as
SELECT
weibo_id

from
(
SELECT t2.idstr ,t1.weibo_id from
(

  SELECT id1 AS weibo_id,uid
      FROM t_base_uid_tmp
      WHERE ds = 'wid'
)t1
left  join t_base_weibo_user t2 on t1.weibo_id=t2.idstr

)t2 where idstr is null and rand()<0.01;