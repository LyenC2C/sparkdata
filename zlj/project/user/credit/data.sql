


--  inner join 266352505

Drop table t_zlj_t_base_uid_tmp_rank;
create table t_zlj_t_base_uid_tmp_rank as
SELECT

  id1 as id ,uid , ROW_NUMBER() OVER (PARTITION BY uid ORDER BY CAST( regexp_replace(regtime,'.','') as bigint )  DESC) AS rn
from
t_base_uid_tmp t1

left join t_base_user_profile t2 on t1.id1=t2.tb_id and t1.ds='ttinfo' and uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
 ;
-- 328253366
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo' ;
--325857943
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo'  and uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}';


create table t_base_ec_record_dev_new_rong360 as
SELECT
/*+ mapjoin(t2)*/
t2.*
from (
SELECT id from
t_zlj_t_base_uid_tmp_rank t3
join t_rong360_distinct_user t4 on t3.uid =t4.phone_no
)t3 join
t_base_ec_record_dev_new t2 on t2.ds='true'  and t1.id =t2.user_id;

SELECT * from  t_zlj_visual_weibo_baseinfo where location  like '%新疆%' limit 100 ;