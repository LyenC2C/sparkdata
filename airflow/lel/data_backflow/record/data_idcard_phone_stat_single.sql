--用户被查询记录
create table  wl_service.t_lel_record_data_backflow_stat_idcard_dkcate
as
select
case when b.idcard is null or c.idcard is null or d.idcard is null then a.idcard else a.idcard end as idcard,
case when d.times is null then 0 else d.times end as lastest_1m,
case when c.times is null then 0 else c.times end as lastest_3m,
case when b.times is null then 0 else b.times end as lastest_6m,
a.times as lastest_12m
from
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),360))) group by idcard)a
full join
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),180))) group by idcard)b
on a.idcard = b.idcard
full join
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),90))) group by idcard)c
on a.idcard = c.idcard
full join
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),30))) group by idcard)d
on a.idcard=d.idcard

create table  wl_service.t_lel_record_data_backflow_stat_idcard_zxcate
as
select
case when b.idcard is null or c.idcard is null or d.idcard is null then a.idcard else a.idcard end as idcard,
case when d.times is null then 0 else d.times end as lastest_1m,
case when c.times is null then 0 else c.times end as lastest_3m,
case when b.times is null then 0 else b.times end as lastest_6m,
a.times as lastest_12m
from
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),360))) group by idcard)a
full join
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),180))) group by idcard)b
on a.idcard = b.idcard
full join
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),90))) group by idcard)c
on a.idcard = c.idcard
full join
(select idcard,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where idcard <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),30))) group by idcard)d
on a.idcard=d.idcard

create table  wl_service.t_lel_record_data_backflow_stat_phone_dkcate
as
select
case when b.phone is null or c.phone is null or d.phone is null then a.phone else a.phone end as phone,
case when d.times is null then 0 else d.times end as lastest_1m,
case when c.times is null then 0 else c.times end as lastest_3m,
case when b.times is null then 0 else b.times end as lastest_6m,
a.times as lastest_12m
from
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),360))) group by phone)a
full join
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),180))) group by phone)b
on a.phone = b.phone
full join
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),90))) group by phone)c
on a.phone = c.phone
full join
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '贷款类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),30))) group by phone)d
on a.phone=d.phone

create table  wl_service.t_lel_record_data_backflow_stat_phone_zxcate
as
select
case when b.phone is null or c.phone is null or d.phone is null then a.phone else a.phone end as phone,
case when d.times is null then 0 else d.times end as lastest_1m,
case when c.times is null then 0 else c.times end as lastest_3m,
case when b.times is null then 0 else b.times end as lastest_6m,
a.times as lastest_12m
from
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),360))) group by phone)a
full join
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),180))) group by phone)b
on a.phone = b.phone
full join
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),90))) group by phone)c
on a.phone = c.phone
full join
(select phone,count(1) as times from wl_service.t_lel_record_data_backflow_combined  where phone <> '' and cate = '征信类' and  (to_date(`date`) > to_date(date_sub(to_date("2017-04-06"),30))) group by phone)d
on a.phone=d.phone

create table wl_service.t_lel_record_data_backflow_stat_phone_idcard
as
select * from
(
select *,"贷款类" as cate from
(select * from wl_service.t_lel_record_data_backflow_stat_idcard_dkcate)a
union all
select *,"征信类" as cate from
(select * from wl_service.t_lel_record_data_backflow_stat_idcard_zxcate)b
)c1
union all
select * from
(
select *,"贷款类" as cate from
(select * from wl_service.t_lel_record_data_backflow_stat_phone_dkcate)a
union all
select *,"征信类" as cate from
(select * from wl_service.t_lel_record_data_backflow_stat_phone_zxcate)b
)c2


insert overwrite table wl_service.t_lel_record_backflow_phone_idcard partition(ds=201704**)
select * from t_lel_record_data_backflow_stat_phone_idcard