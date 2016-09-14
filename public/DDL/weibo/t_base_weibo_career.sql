
CREATE  TABLE  if not exists t_base_weibo_career (
id bigint COMMENT ' 用户UID ' ,
city string  ,
company string ,
department string ,
send  string   COMMENT '结束时间 ' ,
career_id   string ,
province string ,
start string   COMMENT ' 开始时间 ' ,
visible  string
)
COMMENT '微博用户企业信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



LOAD DATA   INPATH '/user/mc/weibo/career_res/' OVERWRITE INTO TABLE t_base_weibo_career PARTITION (ds='20160830') ;

2448w
select count(1) from t_base_weibo_career;
