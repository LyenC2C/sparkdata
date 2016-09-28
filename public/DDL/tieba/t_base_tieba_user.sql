
CREATE  TABLE  if not exists t_base_tieba_user (
tel string   COMMENT '手机号码 '  ,
tag string   COMMENT '唯一tag' ,
user_name string   COMMENT '用户名 '

)
COMMENT '贴吧用户'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



INSERT OVERWRITE table t_base_tieba_user PARTITION(ds='20160927')
SELECT t2.tag,t2.tel ,t1.name
from
(SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_name' group by uid,id1 ) t1 join
(SELECT uid as tag,id1 as tel  from t_base_uid_tmp where ds='tieba_tel' group by uid,id1 )  t2 on t1.tag=t2.tag ;


SELECT  COUNT(1) from (SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_name' group by uid,id1 ) t1 ;
SELECT  COUNT(1) from (SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_tel' group by uid,id1 ) t1 ;