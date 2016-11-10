use wlbase_dev;
create table if not exists t_wrt_qqweibo_realname(
id String COMMENT '微博id',
nickName String COMMENT '昵称',
realName String COMMENT '真实姓名(noname 为无真实姓名)'
)
COMMENT '腾讯微博真实姓名表';
LOAD DATA  INPATH '/user/wrt/temp/qq_weibo_realname/*' OVERWRITE INTO TABLE t_wrt_qqweibo_realname;