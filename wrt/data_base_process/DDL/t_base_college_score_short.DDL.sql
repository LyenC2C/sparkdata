use wlbase_dev;
create table t_base_college_score_short
(
college string  COMMENT '大学全称',
score bigint  COMMENT '大学分数',
short string COMMENT '大学简称'
)
COMMENT '中国大学信息表';

--大学分数，按照985,211,一本,二本,三本,专科 分数依次从6到1'，其中有些未知学校情况的用“-”表示
--大学简称用\t分隔
-- LOAD DATA  INPATH '/user/wrt/temp/college_score_short' OVERWRITE INTO TABLE t_base_college_score_short;