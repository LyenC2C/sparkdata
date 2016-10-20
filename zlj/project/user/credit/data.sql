


--  inner join 266352505

Drop table t_zlj_t_base_uid_tmp_rank;
create table t_zlj_t_base_uid_tmp_rank as
SELECT
  id1 as id ,uid , ROW_NUMBER() OVER (PARTITION BY uid ORDER BY CAST( regexp_replace(regtime,'.','') as bigint )  DESC) AS rn

from
t_base_uid_tmp t1

left join t_base_user_profile t2 on t1.id1=t2.tb_id and t1.ds='ttinfo'
 ;
-- 328253366
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo' ;