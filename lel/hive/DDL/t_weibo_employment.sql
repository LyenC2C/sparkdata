CREATE TABLE  if not exists wl_base.t_base_weibo_employment_info(
uuid String COMMENT '微博id',
employment_info String COMMENT '职业信息'
)
COMMENT '微博用户职业信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;