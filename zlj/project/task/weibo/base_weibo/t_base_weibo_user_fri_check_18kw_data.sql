
-- 关系数据中 相互专注并且都打通手机号的人
create table t_base_weibo_user_fri_check_18kw_data as
  SELECT t3.id1 ,t3.id2
  from
      t_base_uid_tmp   t4
     join  (
          SELECT  /*+ mapjoin(t2)*/ t1.id1 ,t1.id2
            from   t_base_uid_tmp t2 join
          t_base_weibo_user_fri_bi_friends  t1
              on t1.id1 =t2.id1  and  t2.ds='wid'

    )t3  on t3.id2 =t4.id1 and  t4.ds='wid'
;


-- 转换为对应地index索引
create table t_base_weibo_user_fri_check_18kw_data_rank as
SELECT  id ,num ,row_number() over( order by  num desc) as rank
from
(
SELECT  id, COUNT(1) as num
from
(
SELECT id1 as id
FROM t_base_weibo_user_fri_check_18kw_data

UNION  ALL
SELECT id2 as id
FROM t_base_weibo_user_fri_check_18kw_data
)t group by id

 )t1;


--  保存对应id 以及rank数据，后期还原
create table t_base_weibo_user_fri_check_18kw_data_rank_rename as
SELECT  id1, rank1,id2 , rank as rank2
from
(
SELECT
id1, rank as rank1,t2.id2
 from t_base_weibo_user_fri_check_18kw_data_rank t1 join
t_base_weibo_user_fri_check_18kw_data t2 on t1.id =t2.id1
)t1 join t_base_weibo_user_fri_check_18kw_data_rank t2 on t1.id2=t2.id ;


-- 训练数据
create table t_base_weibo_user_fri_check_18kw_data_louvain  as
SELECT
concat_ws('\t',CAST (rank1 as string),cast(rank2 as string))
FROM t_base_weibo_user_fri_check_18kw_data_rank_rename ;