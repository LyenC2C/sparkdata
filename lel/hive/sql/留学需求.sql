留学需求:

create table wl_service.t_lel_liuxue_20170313_more
as
select A.user_id,A.keywords from 
(select user_id,regexp_extract(title,'留学签证|留学中介|留学咨询|留学课程|留学材料|留学托福|留学雅思|留学sat|留学gre|留学gmat|留学文书|留学必备用品|留学印章|留学论文|留学申请',0)as keywords FROM t_base_ec_record_dev_new  where ds='true'  and title regexp '留学签证|留学中介|留学咨询|留学课程|留学材料|留学托福|留学雅思|留学sat|留学gre|留学gmat|留学文书|留学必备用品|留学印章|留学论文|留学申请' and cast(dsn as int) >20160801)A
left semi join 
(select tb_id from t_base_user_profile_telindex  where tb_location regexp '长沙|广东')B 
on A.user_id=B.tb_id

liuxue yasituofu:

set hive.execution.engine=spark;
create table wl_service.t_lel_yasi_tuofu_liuxue
as
select A.user_id,A.keywords from 
(select distinct user_id,regexp_extract(title,'雅思|托福',0)as keywords FROM wl_base.t_base_ec_record_dev_new  where ds='true'  and title regexp '留学.*雅思|留学.*托福' and cast(dsn as int) >20160801)A
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex  where tb_location regexp '长沙|广东')B 
on A.user_id=B.tb_id;

yasituofu:
set hive.execution.engine=spark;
create table wl_service.t_lel_yasi_tuofu
as
select A.user_id,A.keywords from 
(select distinct user_id,regexp_extract(title,'雅思|托福',0)as keywords FROM wl_base.t_base_ec_record_dev_new  where ds='true'  and title regexp '雅思|托福' and cast(dsn as int) >20160801)A
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex  where tb_location regexp '长沙|广东')B 
on A.user_id=B.tb_id;
GRE...:

set hive.execution.engine=spark;
create table wl_service.t_lel_gragmatsat
as
select A.user_id,A.keywords from 
(select distinct user_id,regexp_extract(title,'雅思|托福',0)as keywords FROM wl_base.t_base_ec_record_dev_new  where ds='true'  and title regexp 'GRE|SAT|GMAT' and cast(dsn as int) >20160801)A
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex  where tb_location regexp '长沙|广东')B 
on A.user_id=B.tb_id;


liuxue multi:
create table wl_service.t_lel_multi_liuxue
as
select A.user_id,A.keywords from 
(select distinct user_id,concat('留学',regexp_extract(title,'文书|材料|中介|咨询|课程|签证|申请',0))as keywords FROM wl_base.t_base_ec_record_dev_new  where ds='true'  and title regexp '留学.*(文书|材料|中介|咨询|课程|签证|申请)' and cast(dsn as int) >20160801)A
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex  where tb_location regexp '长沙|广东')B 
on A.user_id=B.tb_id




