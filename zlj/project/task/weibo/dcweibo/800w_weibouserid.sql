DROP  table t_zlj_tmp;
CREATE  TABLE  t_zlj_tmp as
select id  from

(
SELECT orgin_id  as id  from t_zlj_dc_weibodata_next_user_name_id
UNION  ALL
SELECT user_id   as id   from t_zlj_dc_weibodata_next_user_name_id
)t group by  id  ;


CREATE  TABLE  if not exists t_zlj_dc_800wuser_weibo_info (

idstr             string,
followers_count            string,
friends_count            string,
pagefriends_count           string,
statuses_count             string,
favourites_count            string,
created_at            string,
verified            string,
status_created_at           string,
status_idstr            string,
status_text            string,
status_reposts_count        string,
status_comments_count       string,
status_attitudes_count  string
)
COMMENT 'Dc_800w weibo'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


create table t_zlj_tmp_crawler_userid_weiboid as
idstr,status_idstr
SELECT from t_zlj_dc_800wuser_weibo_info ;
LOAD DATA  INPATH '/user/zlj/tmp/dc_800w_userinfo_800w_20161013.data' OVERWRITE INTO TABLE t_zlj_dc_800wuser_weibo_info PARTITION (ds='20151023')



-- 提取3w 用户数据做种子用户

DROP  table t_zlj_tmp_dc_3wuserId ;
create table t_zlj_tmp_dc_3wuserId as
SELECT * from
(
SELECT idstr ,rt  ,ROW_NUMBER() OVER (PARTITION BY 1  ORDER BY rt  DESC) AS rn
from
(
	SELECT
cast(uid as BIGINT )	as rt,
	id1 as weiboid ,
	id2 as comments
	from t_base_uid where ds='dc_rt_test'
	)t1 join t_zlj_dc_800wuser_weibo_info t2 on t1.weiboid =t2.status_idstr
)t2
where rn>100 and rn<201000 ;


