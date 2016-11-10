use wlbase_dev;
create table t_base_weibo_blackuser
(
id string COMMENT '微博id',
username string COMMENT '用户名',
weiboid string COMMENT '微博id',
weibotext string  COMMENT '微博内容',
ts string COMMENT '创建时间戳'
)
COMMENT '黑名单用户微博内容';

-- LOAD DATA  INPATH '/user/wrt/temp/weibo_urlchuli' OVERWRITE INTO TABLE t_base_weibo_blackuser;