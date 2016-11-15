
CREATE  TABLE  if not exists t_base_weibo_usertag (
id bigint COMMENT ' 用户UID ' ,
tags string COMMENT '标签'
)
COMMENT '微博用户标签'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA   INPATH '/user/mc/weibo/tag_res2/' OVERWRITE INTO TABLE t_base_weibo_usertag PARTITION (ds='20160830') ;
LOAD DATA   INPATH '/user/mc/weibo/user_tag' OVERWRITE INTO TABLE t_base_weibo_usertag PARTITION (ds='20161115') ;


select  tag, COUNT(1) from t_base_weibo_usertag lateral view explode(split(tags,'\002')) tt as tag where LENGTH(tag)>1
group by tag
;

1003329587      10:旅游 20160830
1042639012      1994:自信3551:喜欢音乐719:快乐1725:90后3743:活泼开朗11624:努力3706:外贸6027:爱笑3865:巨蟹7229:爱吃      20160830
1042639012      1994:自信3551:喜欢音乐719:快乐1725:90后3743:活泼开朗11624:努力3706:外贸6027:爱笑3865:巨蟹7229:爱吃      20160830
1003329587      10:旅游 20160830