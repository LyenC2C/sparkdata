

405497
SELECT count(1) from t_zlj_dc_weibodata_3w_username_id t1 join
   t_zlj_dc_weibodata t2   on  t2.ds=20161018 and t1.orgin_id=user_id ;



--  原始数据842131
SELECT count(1) from t_zlj_dc_weibodata where ds=2016101  ;


SELECT mid  from t_zlj_dc_weibodata_final_3w_buquan_1019 group by mid HAVING count(1)>1 ;

SELECT  * from t_zlj_dc_weibodata_final_3w_buquan_1019 where mid =4031598767042971 ;


SELECT  count(1) from t_zlj_dc_weibodata where ds=20161019 ;


SELECT  mid,count(1) as num from t_zlj_dc_weibodata_final_3w_buquan_1019 group by mid ;

-- 补全id
DROP  table t_zlj_dc_weibodata_final_3w_buquan_1119;
create table t_zlj_dc_weibodata_final_3w_buquan_1119 as
SELECT
   mid,
          CASE WHEN next_user_name_id IS NOT NULL
            THEN next_user_name_id
          ELSE orgin_id end  AS orgin_id,
          user_id,
          time_cut, content
from
(
  SELECT *,NULL as next_user_name_id
    from t_zlj_dc_weibodata where length(next_user_name)<2 and ds=20161119
    UNION ALL
  SELECT
    t1.*,
    t2.idstr AS next_user_name_id
  FROM t_zlj_dc_weibodata t1 JOIN t_base_weibo_user t2 ON length(t1.next_user_name)>=2
                 and t1.next_user_name = t2.screen_name and t1.ds=20161119
) t
;


--

SELECT orgin_id, sum(num_cut)
FROM
(
  SELECT  t1.mid ,t1.num -t2.num as num_cut ,orgin_id
from
  (SELECT mid  ,count(1) as num  from t_zlj_dc_weibodata_3w_rs_1019 group by  mid )t1 join
(select mid ,orgin_id ,count(1) as num from t_zlj_dc_weibodata where ds=20161019 group by mid ,orgin_id )t2
on t1.mid =t2.mid
  )t
    group by orgin_id  ;


-- 数据量少了比较多

--id 转码， 过滤掉不在800wid中的数据 ，产出转发结果
-- 产出数据量 2953683
DROP   TABLE t_zlj_dc_weibodata_3w_rs_1019;
create table t_zlj_dc_weibodata_3w_rs_1019 as
SELECT mid,
  t3.origin_id_indeex ,
  t4.rn as user_id_index ,time_cut, content
FROM

(
SELECT mid,
  t2.rn as origin_id_indeex ,
  user_id ,time_cut, content
FROM
t_zlj_dc_weibodata_final_3w_buquan_1019  t1
  join t_zlj_dc_weibodata_3w_username_id t2  on t1.orgin_id=t2.orgin_id
)t3 join t_zlj_dc_weibodata_3w_username_id t4 on t3.user_id =t4.orgin_id ;



DROP   TABLE t_zlj_dc_weibodata_3w_rs_1119;
create table t_zlj_dc_weibodata_3w_rs_1119 as
SELECT mid,
  t3.origin_id_indeex ,
  t4.rn as user_id_index ,time_cut, content
FROM

(
SELECT mid,
  t2.rn as origin_id_indeex ,
  user_id ,time_cut, content
FROM
t_zlj_dc_weibodata_final_3w_buquan_1119  t1
  join t_zlj_dc_weibodata_3w_username_id_inc_1119 t2  on t1.orgin_id=t2.orgin_id
)t3 join t_zlj_dc_weibodata_3w_username_id_inc_1119 t4 on t3.user_id =t4.orgin_id ;

SELECT  mid, count(1) from t_zlj_dc_weibodata_3w_rs_1019 group by  mid ;






-- 下面数据不用
-----------------------------------------------------------------------

-- DROP  TABLE t_zlj_dc_weibodata_juesai_3w_username_id_index ;
--   create TABLE t_zlj_dc_weibodata_juesai_3w_username_id_index as
-- select  orgin_id , ROW_NUMBER() OVER (PARTITION BY id ORDER BY orgin_id  DESC) AS rn
-- from
-- (
-- SELECT   CAST(orgin_id as bigint) orgin_id,1 as id
-- FROM
-- (
-- SELECT    orgin_id  from t_zlj_dc_weibodata where ds=20161018
-- UNION  ALL
-- SELECT  user_id  orgin_id  from t_zlj_dc_weibodata  where ds=20161018
-- )t1
-- group by  orgin_id
-- )t2 ;
--
--
-- DROP TABLE t_zlj_dc_tmp  ;
-- CREATE TABLE  t_zlj_dc_tmp as
--   SELECT
--               rn AS orgin_index,
--               ids
--             FROM t_zlj_dc_weibodata_juesai_3w_username_id_index t1 JOIN
--               t_base_weibo_user_fri t2 ON t1.orgin_id = t2.id;
--
-- -- 关联关系表
-- -- 然后拆解ids ，ids 和原始id取交集,再汇集
-- --期间重新编码 用rn代替
--
-- drop table t_t_zlj_dc_weibodata_juesai_link_reindex ;
-- create table t_t_zlj_dc_weibodata_juesai_link_reindex as
-- SELECT
--   orgin_index,
--   concat_ws(',', collect_set(watch_index)) as index_ids
-- FROM
--   (
--     SELECT
--       t1.orgin_index,
--      CAST( t2.rn AS string)  AS watch_index
--     FROM
--       (
--         SELECT
--           CAST(orgin_index AS string) orgin_index,
--              watch_id
--         FROM
--           (
--             SELECT
--               rn AS orgin_index,
--               ids
--             FROM t_zlj_dc_weibodata_juesai_3w_username_id_index t1 JOIN
--               t_base_weibo_user_fri t2 ON t1.orgin_id = t2.id
--           ) t LATERAL VIEW explode(split(ids, ',')) tt as  watch_id
--       ) t1 JOIN t_zlj_dc_weibodata_juesai_3w_username_id_index t2 ON t1.watch_id = t2.orgin_id
--
--   ) t3
-- GROUP BY orgin_index ;