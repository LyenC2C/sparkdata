use wlbase_dev;

CREATE  TABLE  if not exists t_qqweibo_user_school(
id String COMMENT '微博id',
school_info String COMMENT '学历相关信息（各个学历由\003分隔，每个学历信息由\002分隔）'
)
COMMENT 'qq微博用户学历学校表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;