
CREATE  TABLE  if not exists t_base_weibo_user_fri (
id  bigint   COMMENT ' 用户UID ',
ids  string  COMMENT ' 关系列表 用,分割 '

)
COMMENT '微博用户粉丝'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



LOAD DATA   INPATH '/user/zlj/tmp/rel_fri.json' OVERWRITE INTO TABLE t_base_weibo_user_fri PARTITION (ds='20160830') ;


SELECT school,COUNT(1)  from t_base_weibo_user_fri group by school ;

-- 行变列
select id, fid  from t_base_weibo_user_fri lateral view explode(split(ids,',')) tt as fid limit 10;


数量
661353423


--
-- def fun(line):
--     try:
--         ob= json.loads(line)
--         return [str(ob['uid']),','.join([str(i) for i  in ob['ids']])]
--     except:return None
-- sc.textFile('/data/develop/sinawb/rel_fri.json.20160401').map(lambda x:fun(x)).filter(lambda x:x!=None)\
--     .map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/rel_fri.json')
