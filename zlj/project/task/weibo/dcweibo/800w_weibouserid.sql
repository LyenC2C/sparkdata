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



LOAD DATA  INPATH '/user/zlj/tmp/dc_800w_userinfo_800w_20161013.data' OVERWRITE INTO TABLE t_zlj_dc_800wuser_weibo_info PARTITION (ds='20151023')
