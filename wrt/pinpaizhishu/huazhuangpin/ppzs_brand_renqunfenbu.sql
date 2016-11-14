--中间表
drop table wlservice.t_wrt_tmp_ppzs_fenbu;
create table wlservice.t_wrt_tmp_ppzs_fenbu AS
select
t1.brand_id,t1.user_id,t2.qq_gender,
split(t2.tel_loc,'\\s+')[0] as province,
split(t2.tel_loc,'\\s+')[1] as city,
t2.qq_age FROM
(select user_id,brand_id from wlservice.ppzs_brandid_feed group by user_id,brand_id)t1
JOIN
(select tb_id,qq_gender,tel_loc,qq_age from wlbase_dev.t_base_user_profile)t2
ON
t1.user_id = t2.tb_id
WHERE
qq_gender = 0 or qq_gender = 1 or length(tel_loc) > 2 or (qq_age > 10 and qq_age < 70)

split(tel_loc,'\\s+')[0] as province,split(tel_loc,'\\s+')[1] as city


--性别,0男1女

create table wlservice.ppzs_brandid_gender AS
select brand_id,qq_gender FROM wlservice.t_wrt_tmp_ppzs_fenbu where qq_gender = 0 or qq_gender = 1
group by brand_id,qq_gender;

--省份分布

create table wlservice.ppzs_brandid_province AS
ROW_NUMBER() OVER (PARTITION BY t1.brand_id ORDER BY t2.now_sold DESC) as rn
select brand_id,province,count(1) as  FROM wlservice.t_wrt_tmp_ppzs_fenbu where length(tel_loc) > 2

