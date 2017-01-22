CREATE  TABLE  if not exists t_base_weibo_user_keywords(
user_id string comment '用户id',
keywords string comment '用户关键词及权值（关键词用tab隔开，每个用户最多20个关键词）'
)
COMMENT '微博用户关键词'
PARTITIONED BY  (ds STRING )
--LOAD DATA  INPATH '/user/wrt/temp/weibo_keyword_textrank' OVERWRITE INTO TABLE t_base_user_keywords PARTITION (ds='20161012');

CREATE  TABLE  if not exists t_base_weibo_user_keywords(
user_id string comment '用户id',
keywords string comment '用户关键词及权值（关键词用tab隔开，每个用户最多20个关键词）'
)
COMMENT '微博用户关键词'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

--LOAD DATA  INPATH '/user/wrt/temp/weibo_keyword_textrank' OVERWRITE INTO TABLE t_base_weibo_user_keywords PARTITION (ds='20161201');
