use wlbase_dev;
create table t_base_weiboid_college
(
id string COMMENT '微博id',
college string  COMMENT '大学全称'
)
COMMENT '微博用户的大学映射表';
--多个学校的用\t分隔
-- LOAD DATA  INPATH '/user/wrt/weibo_id_college' OVERWRITE INTO TABLE t_base_weiboid_college;