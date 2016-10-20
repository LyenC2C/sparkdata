


--
-- Insert  /user/zlj/tmp/t_base_weibo_user_fri_tel_table ;
--
-- -- 展开数据
-- t_base_weibo_user_fri
-- -- 互相关注人数
-- t_base_weibo_user_fri_bi_friends
--
--
--
--
--
--
-- create table t_zlj_base_weibo_user_fri_followid_tel like t_base_weibo_user_fri;
--
--
-- LOAD DATA  INPATH '/user/zlj/tmp/t_base_weibo_user_fri_tel_table' OVERWRITE INTO TABLE
-- t_zlj_base_weibo_user_fri_followid_tel PARTITION (ds='20161010') ;
--
-- -- 100603551
--
--
-- create table t_zlj_base_weibo_user_fri_followid_tel_bi_friends as
-- SELECT
-- id1, id2 ,COUNT(1) as num
-- from
-- (
-- select
-- sort_array(array(id,fid))[0]  as id1 ,sort_array(array(id,fid))[1] as id2
-- from t_zlj_base_weibo_user_fri_followid_tel lateral view explode(split(ids,',')) tt as fid
-- )t group by id1,id2  HAVING COUNT(1)>1
-- ;


-- 互相为好友的人 去重关联手机号
-- 58970775
CREATE TABLE t_zlj_visul_weibo_user_fri_followid_tel_bi_friends AS
  SELECT
    weibo_id,
    follow_ids
  FROM t_base_uid_tmp t2
    JOIN
    (
      SELECT
        weibo1                              AS weibo_id,
        concat_ws(',', collect_set(weibo2)) AS follow_ids
      FROM
        (
          SELECT
            id1 weibo1,
            id2 weibo2
            from
            t_base_weibo_user_fri_bi_friends
          UNION ALL
          SELECT
            id2 weibo1,
            id2 weibo2
            from
            t_base_weibo_user_fri_bi_friends
        )t
      GROUP BY weibo1
    ) t1
      ON t1.weibo_id = t2.id1 AND t2.ds = 'wid';



-- 48992604
-- 用户画像产出结果
-- DROP table t_zlj_visul_tel_bi_friends_link_rs;
-- create table t_zlj_visul_tel_bi_friends_link_rs as
--  SELECT
-- t1.weibo_id, weibo_pagerank,
-- t2.profile_image_url,
-- t2.screen_name,
-- t2.location ,
-- t3.tags  ,
--  follow_ids
--   from
--  (
--  SELECT  weibo_id  ,pagerank weibo_pagerank , follow_ids   from
--   t_zlj_visul_weibo_user_fri_followid_tel_bi_friends t1
-- join t_zlj_weibo_pagerank_tel t2 on t1.weibo_id =t2.uid
--  )t1
--    join t_base_weibo_user t2 on t1.weibo_id=t2.idstr
--  left join (SELECT  cast(id as string) id  ,tags from t_base_weibo_usertag group by id ,tags ) t3  on t1.weibo_id= t3.id
--   ;


-- SELECT  * from t_base_weibo_usertag where  idstr  in ('1042639012','1003329587');




select  *  from t_zlj_visul_tel_bi_friends_link_rs where weibo_id=2071271691 ;

SELECT   * from t_base_uid_tmp    where ds='wid' and id1=206068661 ;



SELECT * from t_zlj_visul_tel_bi_friends_link_rs  where weibo_id in (2071271691,2086699707,2090276070,2119070614,2129599255,2138445173,2159700982,2165828850,2197715750,2202754573,2234862093,2238176685,2241720503,2254348830,2254374102,2280626261,2281028625,2315492353,2322318857,2341694011,2352046575,2406897273,2429915524,2452658901,2476526617,2501859697,2536514794,2571383924,2640439343,2643766801,2718806131,2746295594,2770057671,2784312827,2798896525,2828576670,3053741635,3222238160,3231536853,3688882617)





-- 关联pagerank
create table t_zlj_visul_weibo_link_pagerank as
SELECT
id ,weibo_pagerank  ,follow_id ,pagerank follow_pagerank
from
t_zlj_weibo_pagerank_tel   t1
join

(
SELECT  id ,weibo_pagerank ,follow_id
from
(
SELECT  id ,pagerank weibo_pagerank , ids  from t_zlj_base_weibo_user_fri_followid_tel t1
join t_zlj_weibo_pagerank_tel t2 on t1.id =t2.uid
 )t
 lateral view explode(split(ids,',')) tt as follow_id
)   t2 on  t1.uid=t2.follow_id ;



-- 过滤掉followid 不再主id里面
CREATE TABLE t_zlj_visul_weibo_link_pagerank_filter AS
 SELECT
  t1.id AS                  weibo_id,
  round(weibo_pagerank, 2)  weibo_pagerank,
  follow_id,
  round(follow_pagerank, 2) follow_pagerank
 FROM t_zlj_visul_weibo_link_pagerank t1
  JOIN
  (
   SELECT id
   FROM t_zlj_visul_weibo_link_pagerank
   GROUP BY id
  ) t2 ON t1.follow_id = t2.id
;

-- 聚合followid, 产出结果  65814855



create table t_zlj_visul_weibo_link_pagerank_filter_rs as
SELECT
t1.weibo_id, weibo_pagerank,
t2.profile_image_url,
t2.screen_name,
t2.location ,
t3.tags  ,
 follow_ids
from
(


(
SELECT
 weibo_id ,weibo_pagerank ,concat_ws(' ',collect_set( follow_id) ) as follow_ids
 from t_zlj_visul_weibo_link_pagerank_filter
 group  by weibo_id ,weibo_pagerank
)t1 RIGHT join t_zlj_weibo_pagerank_tel t2 on t1.weibo_id=t2.uid

 )t1
 join t_base_weibo_user t2 on t1.weibo_id=t2.idstr
 left join t_base_weibo_usertag on t1.weibo_id=t3.id
 ;



create table t_zlj_visul_weibo_link_pagerank_follows as

select weibo_id ,weibo_pagerank ,
concat_ws(' ',collect_set( follow_id) ) as follows
from
(
SELECT
id as weibo_id , round(weibo_pagerank,2) weibo_pagerank ,follow_id ,round(follow_pagerank,2)  follow_pagerank
from  t_zlj_visul_weibo_link_pagerank
)t group by weibo_id ,weibo_pagerank ;