


# 18705431


#  qq_group_info where ds='school'  1023399
create table t_zlj_qq_find_qq_school  as
select
t2.qq_id ,
 t1.qun_id,
t1.find_schools
from
(
select  *  from qq_group_info where ds='school'
 )t1

 join
qun_member_info t2 on t1.qun_id=t2.qun_id
 group by  t2.qq_id , t1.qun_id, t1.find_schools
;


create table t_zlj_qq_find_qq_school_rst as
select qq_id ,concat_ws('\t',collect_set(qun_id)) qun_ids ,
 concat_ws('\t',collect_set(find_schools))  find_schools from t_zlj_qq_find_qq_school group by qq_id ;

# select COUNT(DISTINCT qq_id ) from t_zlj_qq_find_qq_school ;


# 不再挖掘的学校群里有多少是 已挖掘学校的人加入的
create table t_zlj_qq_tmp as
SELECT

t2.qun_id,t2.qq_id,t1.schools
FROM (select qq_id, concat_ws('\t',collect_set(find_schools))  as schools from t_zlj_qq_find_qq_school group by  qq_id) t1 JOIN

 (
  SELECT t4.*
  FROM
   qun_member_info t4 LEFT JOIN
   (
    SELECT *
    FROM qq_group_info
    WHERE ds = 'school'
   ) t3 ON t3.qun_id = t4.qun_id
  WHERE t3.qun_id IS NULL

 ) t2 ON t1.qq_id = t2.qq_id;

# select  *  from t_zlj_qq_find_qq_school where qq_id in (271720984 ,20282494 ,290264617) ;


create table t_zlj_qq_tmp_find_qq_school_zhuanye  as
select
 t1.qun_id,
 title,
 qun_text,
t1.find_schools
from
(
select * from qq_group_info where ds='zhuanye'

 )t1

 join
 (
  select qun_id
   from
t_zlj_qq_tmp
  group by qun_id
)t2 on t1.qun_id=t2.qun_id
;



# 专业群里 包含学校学生 个数统计
create table t_zlj_qq_tmp_find_qq_school_zhuanye_count_1  as

 SELECT
 qun_id,
 max(title) ,
 max(qun_text),
 max(find_schools),
  concat_ws('\t',collect_set(schools)),
  count(1) as num

 from (
  select
 t1.qun_id,
 t1.title ,
 t1.qun_text,
 t1.find_schools,
   t2.schools
from
(
select * from qq_group_info where ds='zhuanye'

 )t1

 join

t_zlj_qq_tmp
t2
 on t1.qun_id=t2.qun_id

 )tt

 group by qun_id
;



SELECT  * from t_zlj_qq_tmp_find_qq_school_zhuanye_count where num>4 limit 50;

# create table t_zlj_qq_find_qq_school  as


# 26609764
 SELECT count(DISTINCT qq_id)
 FROM
 (select
t2.qq_id
from
t_zlj_qq_tmp_find_qq_school_zhuanye_count  t1

 join
qun_member_info t2 on t1.qun_id=t2.qun_id and t1.num >4

 UNION  all
    select qq_id from t_zlj_qq_find_qq_school
 )t
;


select count(DISTINCT qq_id) from t_zlj_qq_find_qq_school