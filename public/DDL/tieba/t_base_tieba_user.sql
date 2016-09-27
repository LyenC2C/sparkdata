
CREATE  TABLE  if not exists t_base_tieba_user (
tel string   COMMENT '手机号码 '  ,
tag string   COMMENT '唯一tag' ,
user_name string   COMMENT '用户名 '

)
COMMENT '贴吧用户'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



LOAD DATA   INPATH '/user/mc/weibo/career_res/' OVERWRITE INTO TABLE t_base_weibo_career PARTITION (ds='20160830') ;

2448w
select count(1) from t_base_weibo_career;
