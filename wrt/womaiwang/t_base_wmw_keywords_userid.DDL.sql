create table t_base_wmw_keywords_userid
(
keywords string,
user_id string
);
LOAD DATA  INPATH '/user/wrt/temp/womaiwang_tongji' OVERWRITE INTO TABLE t_base_wmw_keywords_userid;