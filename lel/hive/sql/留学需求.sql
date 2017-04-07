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


liuxue multi:
create table wl_service.t_lel_multi_liuxue
as
select A.user_id,A.keywords from 
(select distinct user_id,concat('留学',regexp_extract(title,'文书|材料|中介|咨询|课程|签证|申请',0))as keywords FROM wl_base.t_base_ec_record_dev_new  where ds='true'  and title regexp '留学.*(文书|材料|中介|咨询|课程|签证|申请)' and cast(dsn as int) >20160801)A
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex  where tb_location regexp '长沙|广东')B 
on A.user_id=B.tb_id


高端大米的营销需求:
hive:
create table t_lel_dami_needs_20170313
as
select t3.tb_id,t3.tel_loc from
(
select
user_id,
case when extract_title regexp "斤" then price / (regexp_extract(extract_title,"\\d+",0))
when extract_title regexp "千克|Kg|kg|KG|kG"  then price / ((regexp_extract(extract_title,"\\d+",0))/ 2)
when extract_title regexp "克|g" then price / ((regexp_extract(extract_title,"\\d+",0))/500) end as price
from 
(SELECT 
	user_id,
	regexp_extract(title,"\\d+(斤|kg|KG|Kg|kG|g|G|千克|克)",0) as extract_title,
	price FROM wl_base.t_base_ec_record_dev_new  
	where ds='true' and title regexp '大米' and cast(dsn as int )> 20160801) t1
) t2
join 
(select tb_id,tel_loc from wl_base.t_base_user_profile_telindex where xianyu_gender='1')t3
on t2.user_id = t3.tb_id
where t2.price > 10

impala:
create table t_lel_dami_needs_20170313
as
select t3.tb_id,t3.tel_loc from
(
select
user_id,
case when extract_title regexp "斤" then price / cast(regexp_extract(extract_title,"\\d+",0) as float)
when extract_title regexp "千克|Kg|kg|KG|kG"  then price / (cast(regexp_extract(extract_title,"\\d+",0) as float)/ 2)
when extract_title regexp "克|g" then price / (cast(regexp_extract(extract_title,"\\d+",0) as float)/500) end as price
from  
(SELECT 
	user_id,
	regexp_extract(title,"\\d+(斤|kg|KG|Kg|kG|g|G|千克|克)",0) as extract_title,
	price FROM wl_base.t_base_ec_record_dev_new  
	where ds='true' and title regexp '大米' and cast(dsn as int )> 20160801) t1
) t2
join 
(select tb_id,tel_loc from wl_base.t_base_user_profile_telindex where xianyu_gender='1')t3
on t2.user_id = t3.tb_id
where t2.price > 10

