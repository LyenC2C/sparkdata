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



