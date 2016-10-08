
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
SELECT t2.tel , t2.tag ,t1.name
from
(SELECT uid as tag,concat_ws(' ',collect_set(id1)) as name  from t_base_uid_tmp where ds='tieba_name'  and LENGTH(uid)>2 group by uid ) t1 join
(SELECT uid as tag,concat_ws(' ',collect_set(id1)) as tel  from t_base_uid_tmp where ds='tieba_tel' and  LENGTH(uid)>2  group by uid )  t2 on t1.tag=t2.tag ;


create table t_zlj_tmp as
SELECT  * from t_base_tieba_user where size(split(tel,' '))>1 ;
31320799
SELECT  COUNT(1) from (SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_name' group by uid,id1 ) t1 ;

38838730
SELECT  COUNT(1) from (SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_tel' group by uid,id1 ) t1 ;

create table t_zlj_tmp_1 as
SELECT uid from t_base_uid_tmp where ds='tieba_tel'
union ALL
SELECT uid from t_base_uid_tmp where ds='tieba_name'

31018389
SELECT  COUNT(1) from (SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_name' group by uid ) t1 ;
37714244
SELECT  COUNT(1) from (SELECT uid as tag,id1 as name  from t_base_uid_tmp where ds='tieba_tel' group by uid ) t1 ;

31320799
SELECT  COUNT(1) from t_base_uid_tmp where ds='tieba_name' and  LENGTH(uid)>2;

52198813
SELECT  COUNT(1) from t_base_uid_tmp where ds='tieba_tel' and  LENGTH(uid)>2;