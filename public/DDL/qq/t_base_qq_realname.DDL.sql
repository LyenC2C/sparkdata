CREATE  TABLE  if not exists t_base_qq_realname (
qq string comment 'qq号',
realname string comment '真名'
)
COMMENT 'qq真名表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

--LOAD DATA  INPATH '/user/wrt/temp/qq_realname' OVERWRITE INTO TABLE t_base_qq_realname PARTITION (ds='20161012');

