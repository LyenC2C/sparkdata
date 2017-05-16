credit_bank_distribute:

create table wl_service.t_lel_credit_bank_stat
as
select a.phone,a.platforms,count,b.province,b.city from 
(SELECT phone,substr(phone,1,7) as prefix,concat_ws("|",collect_set(platform)) as platforms,size(collect_set(platform)) as count FROM  t_base_credit_bank where ds = '20170306' and flag = 'True' group by phone,substr(phone,1,7))a
join
(select prefix,province,city from t_base_mobile_loc_new)b
on a.prefix = b.prefix

create table  wl_service.t_lel_credit_bank_stat_res
as
select
case when b.province is null or c.province is null or d.province is null then a.province else a.province end as province,
case when b.city is null or c.city is null or d.city is null then a.city else a.city end as city,
a.have as have,
case when b.one is null then 0 else b.one end as one,
case when c.two is null then 0 else c.two end as two,
case when d.three is null then 0 else d.three end as three
from 
(select province,city,sum(count) as have from t_lel_credit_bank_stat group by province,city)a
full join
(select province,city,sum(count) as one from t_lel_credit_bank_stat where count = 1 group by province,city )b
on a.province = b.province and a.city = b.city
full join
(select province,city,sum(count) as two from t_lel_credit_bank_stat where count = 2 group by province,city)c
on a.province = c.province and a.city = c.city
full join
(select province,city,sum(count) as three from t_lel_credit_bank_stat where count = 3 group by province,city)d
on a.province = d.province and a.city = d.city

or

create table  wl_service.t_lel_credit_bank_stat_res
as
select
a.province,
a.city,
a.have as have,
case when b.one is null then 0 else b.one end as one,
case when c.two is null then 0 else c.two end as two,
case when d.three is null then 0 else d.three end as three
from 
(select province,city,sum(count) as have from t_lel_credit_bank_stat group by province,city)a
full join
(select province,city,sum(count) as one from t_lel_credit_bank_stat where count = 1 group by province,city )b
on a.province = b.province and a.city = b.city
full join
(select province,city,sum(count) as two from t_lel_credit_bank_stat where count = 2 group by province,city)c
on a.province = c.province and a.city = c.city
full join
(select province,city,sum(count) as three from t_lel_credit_bank_stat where count = 3 group by province,city)d
on a.province = d.province and a.city = d.city