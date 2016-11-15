
CREATE  TABLE  if not exists t_base_weibo_edu (
id  bigint   COMMENT ' 用户UID ',
area   int   ,
city   string   ,
department   string COMMENT '部门 '   ,
department_id   int   ,
endYear   int   ,
edu_id   int   ,
is_verified   string   ,
province   int   ,
school   string   COMMENT '学校'  ,
school_id   int   ,
type   int  COMMENT '类型'  ,
visible   int   ,
s_year   int

)
COMMENT '微博用户教育信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- 马驰

LOAD DATA   INPATH '/user/mc/weibo/edu_res/' OVERWRITE INTO TABLE t_base_weibo_edu PARTITION (ds='20160830') ;
LOAD DATA   INPATH '/user/mc/weibo/education' OVERWRITE INTO TABLE t_base_weibo_edu PARTITION (ds='20161111') ;
LOAD DATA   INPATH '/user/mc/weibo/education' OVERWRITE INTO TABLE t_base_weibo_edu PARTITION (ds='20161115') ;


SELECT school,COUNT(1)  from t_base_weibo_edu group by school ;

6858w
select count(1) from t_base_weibo_edu;