use wlbase_dev;

CREATE  TABLE  if not exists t_qqweibo_user_school(
id String COMMENT '微博id',
school_info String COMMENT '工作相关信息（各个工作由\003分隔，每个工作信息由\002分隔）'
)
COMMENT 'qq微博用户工作功能公司表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;