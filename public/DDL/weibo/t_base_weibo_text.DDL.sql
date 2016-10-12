CREATE  TABLE  if not exists t_base_weibo_text (
mid string comment '微博内容id',
user_id string comment '用户id',
created_at string comment '微博创建时间',
text string comment '微博内容',
source string comment '微博来源（什么手机发的）',
favorited string comment '是否已收藏',
truncated string comment '是否被截断',
thumbnail_pic string comment '缩略图片地址',
geo string comment '地理信息',
reposts_count string comment '转发数',
comments_count string comment '评论数',
attitudes_count string comment '表态数',
weibo_type string comment '微博类型（0：普通微博，1：私密微博，3：指定分组微博，4：密友微博）',
isLongText string comment '是否长微博',
retweeted_status string comment '转发的原微博id,userid和微博内容（用\002隔开，没有转发时为‘-’）',
ts string comment '采集时间戳'

)
COMMENT '微博具体内容'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

--LOAD DATA  INPATH '/user/wrt/temp/weibo_text' OVERWRITE INTO TABLE t_base_weibo_text PARTITION (ds='20161012');