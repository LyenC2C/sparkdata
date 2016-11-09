--  inner join 266352505
-- 去重排序 tel
Drop table t_zlj_t_base_uid_tmp_rank;
create table t_zlj_t_base_uid_tmp_rank as
SELECT

  id1 as id ,uid , ROW_NUMBER() OVER (PARTITION BY uid ORDER BY CAST( regexp_replace(regtime,'.','') as bigint )  DESC) AS rn
from
(
SELECT uid ,id1  from
t_base_uid_tmp where  uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
and ds='ttinfo'
)t1
left join t_base_user_profile t2 on t1.id1=t2.tb_id
 ;