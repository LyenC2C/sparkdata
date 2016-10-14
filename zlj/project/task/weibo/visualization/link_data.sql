


-- 筛选相互关联的数据
create table t_zlj_visul_weibo_user_fri_bi_friends_step1 as
SELECT  id1 weibo_id , concat_ws(',',collect_set(id2))  watch_ids
FROM
t_base_weibo_user_fri_bi_friends where num > 1
group by id1;



-- 数据做笛卡尔及 ，sql数据量太大跑失败了。改用spark处理，然后加载到t_base_uid
create table t_zlj_visul_weibo_user_fri_bi_friends_step2 as
SELECT  weibo_id ,concat_ws('-',watch_id1,watch_id2) as linkid
from
 t_zlj_visul_weibo_user_fri_bi_friends_step1
lateral VIEW explode(split(watch_ids,',')) tt AS watch_id1
lateral VIEW explode(split(watch_ids,',')) tt AS watch_id2 ;
where watch_id1<>watch_id2 ;

-- create table t_zlj_visul_weibo_user_fri_bi_friends_step2 as
--
-- SELECT *
-- from
-- (
-- SELECT t1.weibo_id , t1.watch_id as id1 ,t2.watch_id as id2
-- from
-- t_zlj_visul_weibo_user_fri_bi_friends_step1 t1
-- join t_zlj_visul_weibo_user_fri_bi_friends_step1 t2 on t1.weibo_id=t2.weibo_id
-- ) t where id1<>id2 ;

-- create table t_zlj_visul_weibo_user_fri_bi_friends_step2 as
--
--
-- SELECT t1.weibo_id ,concat_ws('-',watch_id1,watch_id2) as linkid
-- from
-- (select * from t_zlj_visul_weibo_user_fri_bi_friends_step1 limit 1000)
--  t1
-- lateral VIEW explode(split(watch_ids,',')) tt AS watch_id1
-- lateral VIEW explode(split(watch_ids,',')) tt AS watch_id2
--  where watch_id1<>watch_id2 ;


  数据做笛卡尔
LOAD DATA  INPATH '/user/zlj/tmp/weibo_bi_fri_tmp/' OVERWRITE INTO TABLE t_base_uid  PARTITION (ds='wb_fri_linkflat')
CREATE  TABLE  t_zlj_tmp_test1 as
SELECT *
FROM t_base_uid where
ds='wb_fri_linkflat'  limit 100000000;


create table t_zlj_visul_weibo_user_fri_bi_friends_step3  as
SELECT  t2.uid,t2.link from
(
SELECT concat_ws('\002',id1,id2) as link
from t_base_weibo_user_fri_bi_friends

)t1 join
(
SELECT  uid, concat_ws('\002' ,sort_array(split(id1,'\002'))[0] ,sort_array(split(id1,'\002'))[1] )  as link
  from t_base_uid  where ds='wb_fri_linkflat'
  )t2  on t1.link=t2.link  ;

-- SELECT  uid,  sort_array(split(id1,'\002'))[0]
--   from t_base_uid  where ds='wb_fri_linkflat' limit  10


drop table t_zlj_tmp_wb_fri_linkflat;
  create table t_zlj_tmp_wb_fri_linkflat as
  SELECT  hashvalue
  from
  (
  SELECT concat_ws('',substr(id1,1,4 ),substr(id2,1,4 ))  hashvalue  from t_base_weibo_user_fri_bi_friends
  )t group by hashvalue ;



  SELECT /*+ mapjoin(t2)*/ t2.uid , concat_ws('\002',link1 ,link2) as link  from t_zlj_tmp_wb_fri_linkflat t1
  join
  (
  SELECT  uid, sort_array(split(id1,'\002'))[0] link1 ,sort_array(split(id1,'\002'))[1] link2
  from t_base_uid  where ds='wb_fri_linkflat'
  where sort_array(split(id1,'\002'))[0]<>sort_array(split(id1,'\002'))[1]
  )t2 on t1.hashvalue=concat_ws('',substr(link1,1,4 ),substr(link2,1,4 ))