--中间表
drop table wlservice.t_wrt_tmp_ppzs_fenbu;
create table wlservice.t_wrt_tmp_ppzs_fenbu AS
select
t1.brand_id,t1.user_id,t2.qq_gender,
split(t2.tel_loc,'\\s+')[0] as province,
split(t2.tel_loc,'\\s+')[1] as city,
tel_loc,
t2.qq_age FROM
(select user_id,brand_id from wlservice.ppzs_brandid_feed group by user_id,brand_id)t1
JOIN
(select tb_id,qq_gender,tel_loc,qq_age from wlbase_dev.t_base_user_profile)t2
ON
t1.user_id = t2.tb_id
WHERE
qq_gender = 0 or qq_gender = 1 or length(tel_loc) > 2 or (qq_age > 10 and qq_age < 70)


--性别,0男1女
drop table wlservice.ppzs_brandid_gender;
create table wlservice.ppzs_brandid_gender AS
select brand_id,qq_gender,count(1) as ct FROM wlservice.t_wrt_tmp_ppzs_fenbu where qq_gender = 0 or qq_gender = 1
group by brand_id,qq_gender;

--省份分布
drop table wlservice.ppzs_brandid_pronvince
create table wlservice.ppzs_brandid_province AS
select t1.brand_id,t1.province,t1.ct,t2.brand_sum,t1.rn from
(
select
brand_id,province,ct,rn from
(
SELECT
brand_id,province,ct,
ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY ct DESC) as rn
from
(
select brand_id,province,count(1) as ct  FROM wlservice.t_wrt_tmp_ppzs_fenbu where length(tel_loc) > 2 group by brand_id,province
)t
)tt
where rn < 21
)t1
JOIN
(
select brand_id,count(1) as brand_sum from wlservice.t_wrt_tmp_ppzs_fenbu where length(tel_loc) > 2 group by brand_id
)t2
ON
t1.brand_id = t2.brand_id
--城市分布
drop table wlservice.ppzs_brandid_city;
create table wlservice.ppzs_brandid_city AS
select t1.brand_id,t1.city,t1.ct,t2.brand_sum,t1.rn from
(
select
brand_id,city,ct,rn from
(
SELECT
brand_id,city,ct,
ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY ct DESC) as rn
from
(
select brand_id,city,count(1) as ct  FROM wlservice.t_wrt_tmp_ppzs_fenbu where length(tel_loc) > 2 group by brand_id,city
)t
)tt
where rn < 21
)t1
JOIN
(
select brand_id,count(1) as brand_sum from wlservice.t_wrt_tmp_ppzs_fenbu where length(tel_loc) > 2 group by brand_id
)t2
ON
t1.brand_id = t2.brand_id;


--年龄分布

create table wlservice.ppzs_brandid_age AS
select
brand_id,age_fenbu,ct,rn from
(
SELECT
brand_id,age_fenbu,ct,
ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY ct DESC) as rn
from
(
select brand_id,
case
when qq_age < 15 then "under15"
when (qq_age>=15 and qq_age<20) then "15to20"
when (qq_age>=20 and qq_age<25) then "20to25"
when (qq_age>=25 and qq_age<30) then "25to30"
when (qq_age>=30 and qq_age<35) then "30to35"
when (qq_age>=35 and qq_age<40) then "35to40"
when (qq_age>=40 and qq_age<45) then "40to45"
when (qq_age>=45 and qq_age<50) then "45to50"
when (qq_age>=50 and qq_age<=60) then "50to60"
END as age_fenbu,
count(1) as ct  FROM wlservice.t_wrt_tmp_ppzs_fenbu where qq_age > 10 and qq_age < 60
group by brand_id,
case
when qq_age < 15 then "under15"
when (qq_age>=15 and qq_age<20) then "15to20"
when (qq_age>=20 and qq_age<25) then "20to25"
when (qq_age>=25 and qq_age<30) then "25to30"
when (qq_age>=30 and qq_age<35) then "30to35"
when (qq_age>=35 and qq_age<40) then "35to40"
when (qq_age>=40 and qq_age<45) then "40to45"
when (qq_age>=45 and qq_age<50) then "45to50"
when (qq_age>=50 and qq_age<=60) then "50to60"
END
)t
)tt