use wlbase_dev;
create table t_base_weiboid_career
(
id string COMMENT '微博id',
company string  COMMENT '公司名',
department string COMMENT '职位名'
)
COMMENT '微博用户的职业映射表';

--公司名和职位名，如果有多个用'\t'隔开，其中职位名如果异常或者不存在，那么用“-”代替
-- LOAD DATA  INPATH '/user/wrt/weiboid_career' OVERWRITE INTO TABLE t_base_weiboid_career;