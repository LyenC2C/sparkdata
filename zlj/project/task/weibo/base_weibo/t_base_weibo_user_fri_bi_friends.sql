


LOAD DATA  INPATH '/user/zlj/tmp/weibo_friendships_20161101' OVERWRITE INTO TABLE t_base_weibo_user_fri PARTITION (ds='20161101')

--  计算相互关注人数
create table t_base_weibo_user_fri_bi_friends as
SELECT
id1, id2 ,COUNT(1) as num
from
(
select
sort_array(array(id,fid))[0]  as id1 ,sort_array(array(id,fid))[1] as id2
from t_base_weibo_user_fri where ds = 20160902 lateral view explode(split(ids,',')) tt as fid
)t group by id1,id2  HAVING COUNT(1)>1
;


  create table t_base_weibo_user_fri_bi_friends_v12 as
  SELECT
  id1, id2 ,COUNT(1) as num
  from
  (
  select
  sort_array(array(id,fid))[0]  as id1 ,sort_array(array(id,fid))[1] as id2
  from (select * from t_base_weibo_user_fri where ds=20161102 and length(ids)>0 )t lateral view explode(split(ids,',')) tt as fid
  )t group by id1,id2  HAVING COUNT(1)>1
  ;

-- 去重
insert overwrite table t_base_weibo_user_fri partition(ds='20161102')
select id ,ids
  from
(
select id ,ids ,row_number()  OVER (PARTITION BY id ORDER BY 1 desc) as rn
 from t_base_weibo_user_fri where ds=20161101
)t
;
-- 测试

SELECT * from t_base_weibo_user_fri where ds=20161101 and id in (1000009700,1916655407) ;

重复数 165691458
SELECT count(1) from  (SELECT id,count(1) from t_base_weibo_user_fri where ds=20161101 group by id HAVING count(1)>1)t;

--
--
--
--









-- pagerank  侠哥测试IV
create table t_zlj_weibo_pagerank_tel_test as
SELECT  count(1)
 from t_base_uid_tmp
  t1
join

  (
  select t1.id1 as weibo_id  ,t1.uid as tel
    from
  t_base_uid_tmp t1 join wlfinance.t_hx_total_user t2 on t1.ds='wid' and  t1.uid =t2.phone_no
    group by t1.uid ,t1.id1
  )t2
  on t1.uid =t2.weibo_id and t1.ds = 'weibo_pagerank'
  ;