

CREATE  TABLE  if not exists t_base_weibo_user_fri_bi(
weibo_id string comment 'weibo_id',
follow_ids string comment '关注用户id列表'
)
COMMENT '微博用户关键词'
PARTITIONED BY  (ds STRING )
--LOAD DATA  INPATH '/user/wrt/temp/weibo_keyword_textrank' OVERWRITE INTO TABLE t_base_user_keywords PARTITION (ds='20161012');

