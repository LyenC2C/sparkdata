datamart_backflow

count(1):9192501
api_type:79
enc_m:2282484
interface:121
match_1:5648166 match_0:3544335 
repeat_0:8884784 repeat_1:307717
success_0:562242 success_1:8630259

max_time -> 2017/4/6 4:23:26


create table wl_base.t_base_datamart_backflow_parquet_lessfields stored as parquet
as
select app_key,app_key_param,from_unixtime(cast(ts_request as bigint),'yyyy-MM-dd') as date,interface,api_type,params_less
from t_base_datamart_backflow_parquet where api_type <>'tel_basics'  


("card","accountNo","accountno","name","iname","idCard","cardNum","enc_m","phone","mobile","ownerMobile","bankpremobile")



create table t_lel_datamart_backflow_filtered
as
select * from t_lel_datamart_backflow 
where 
(name not regexp '[A-Za-z\\d]' or name is null or name = '')
and
(length(phone) =11 or phone is null or phone = '')
and
(length(idcard) = 18  or idcard is null or idcard = '')
and
(length(idbank) = 16 or length(idbank) = 19 or idbank is null or idbank = '')


drop table t_lel_datamart_backflow_combined;
create table wl_service.t_lel_datamart_backflow_combined
as
select 
b.company,a.phone,a.idbank,a.idcard,a.name,a.`date`,b.cate from
(select
app_key,`date`,
case when phone is null then '' else phone end as phone,
case when idbank is null then '' else idbank end as idbank,
case when idcard is null then '' else idcard end as idcard,
case when name is null then '' else name end as name
from t_lel_datamart_backflow_filtered)a
join
(select * from wl_service.t_lel_datamart_backflow_company_cates)b
on a.app_key=b.app_key


drop table wl_service.t_lel_datamart_backflow_stat_all;
create table wl_service.t_lel_datamart_backflow_stat_all
as
select * from 
(
select *,"贷款类" as cate from 
(select phone,idbank,idcard,name,max(to_date(`date`)) as latest, count(1)as times from t_lel_datamart_backflow_combined  where cate = '贷款类' group by phone,idbank,idcard,name)a
union all
select *,"征信类" as cate from
(select phone,idbank,idcard,name,max(to_date(`date`)) as latest, count(1)as times from t_lel_datamart_backflow_combined  where cate = '征信类' group by phone,idbank,idcard,name)b
)c


create table  t_lel_datamart_backflow_stat_idcard_dkcate
as
select
case when b.idcard is null or c.idcard is null or d.idcard is null then a.idcard else a.idcard end as idcard,
case when d.times is null then 0 else d.times end as lastest_1m,
case when c.times is null then 0 else c.times end as lastest_3m,
case when b.times is null then 0 else b.times end as lastest_6m,
a.times as lastest_12m
from 
(select idcard,count(1) as times from t_lel_datamart_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),360))) group by idcard)a
full join
(select idcard,count(1) as times from t_lel_datamart_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),180))) group by idcard)b
on a.idcard = b.idcard
full join
(select idcard,count(1) as times from t_lel_datamart_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),90))) group by idcard)c
on a.idcard = c.idcard
full join
(select idcard,count(1) as times from t_lel_datamart_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),30))) group by idcard)d
on a.idcard=d.idcard


drop table t_lel_datamart_backflow_stat_phone_idcard_stat;
create table wl_service.t_lel_datamart_backflow_stat_phone_idcard_stat
as
select * from 
(
select *,"贷款类" as cate from 
(select * from t_lel_datamart_backflow_stat_idcard_dkcate)a
union all
select *,"征信类" as cate from
(select * from t_lel_datamart_backflow_stat_idcard_zxcate)b
)c1
union all
select * from 
(
select *,"贷款类" as cate from 
(select * from t_lel_datamart_backflow_stat_phone_dkcate)a
union all
select *,"征信类" as cate from
(select * from t_lel_datamart_backflow_stat_phone_zxcate)b
)c2 



product:
wl_service.t_lel_datamart_backflow_stat_all
wl_service.t_lel_datamart_backflow_stat_phone_idcard_stat




######
stat:#
######

all_records:3360902
select count(1) from t_lel_datamart_backflow where length(phone) =11 
:3294849
select count(1) from t_lel_datamart_backflow where length(phone) =11 and phone regexp '\\d+'
:3294849
select count(1) from t_lel_datamart_backflow where length(phone) <> 11 and phone is not null
:404

select count(1) from t_lel_datamart_backflow where length(idcard) = 18   
:102348
select count(1) from t_lel_datamart_backflow where length(idcard) = 18  and idcard regexp '\\d+'
:102348
select count(1) from t_lel_datamart_backflow where length(idcard) <> 18 and idcard is not null
:18767


select count(1) from t_lel_datamart_backflow where length(idbank) = 16 or length(idbank) = 19   
:58655
select count(1) from t_lel_datamart_backflow where length(idbank) = 16 or length(idbank) = 19 and idbank regexp '\\d+'
:58654
select count(1) from t_lel_datamart_backflow where length(idbank) <> 16 and length(idbank) <> 19  and idbank is not null
:918


select count(1) from t_lel_datamart_backflow_combined
:2282856








	