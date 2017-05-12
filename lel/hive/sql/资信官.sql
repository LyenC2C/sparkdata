资信官-金融产品推荐平台的测试需求

limit:成都 number:per 200




1.投资理财偏好人群
set hive.execution.engine=spark;
set hive.merge.sparkfiles=true;
create table wl_service.t_lel_touzilicai_lv1_20170401
as
select a.user_id from
(SELECT distinct(user_id) FROM wl_base.t_base_ec_record_dev_new 
where ds='true' 
and title regexp "投资|理财")a
left semi join
(select user_id from wl_service.t_lel_underwear_userids)b
on a.user_id=b.user_id

create table wl_service.t_lel_touzilicai_zixinguan_20170401
as
select a.user_id from 
(SELECT user_id from wl_service.t_lel_touzilicai_lv1_20170401)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id=b.tb_id

2.母婴人群
set hive.execution.engine=spark;
set hive.merge.sparkfiles=true;
create table wl_service.t_lel_muying_lv1_20170401
	as
	select a.user_id from
(select user_id from wl_usertag.usertag_consume_wlid_pianhao_monthall where ismuying_monthall='1')a
left semi join
(select user_id from wl_service.t_lel_underwear_userids)b
on a.user_id=b.user_id

create table wl_service.t_lel_muyin_zixinguan_20170401
as
select a.user_id from 
(SELECT user_id from wl_service.t_lel_muying_lv1_20170401)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id=b.tb_id


3.汽车偏好人群

create table wl_service.t_lel_car_lv1_20170401
	as
	select a.user_id from
(select user_id from wl_usertag.usertag_consume_wlid_pianhao_monthall where iscar_monthall='1')a
left semi join
(select user_id from wl_service.t_lel_underwear_userids)b
on a.user_id=b.user_id

create table wl_service.t_lel_car_zixinguan_20170401
as
select a.user_id from 
(SELECT user_id from wl_service.t_lel_car_lv1_20170401)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id=b.tb_id


4.信用卡人群
create table wl_service.t_lel_zixinguan_creditcard_20170401
as
select distinct a.phone from
(select phone,substr(phone,0,7) as prefix  from wl_base.t_base_credit_bank where flag='True' and ds='20170306')a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix=b.prefix
5.多平台借贷人群
create table wl_service.t_lel_zixinguan_jiedai_20170401
as
select distinct a.phone from
(select phone,substr(phone,0,7) as prefix  from wl_base.t_base_multiplatform where flag='True' and ds='20170227')a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix=b.prefix



product:

1.
create table t_lel_touzilicai_zixinguan_20170401_phoneid_200
as
select phone from t_lel_touzilicai_zixinguan_20170401_phoneid order by rand() limit 200
2. 
create table t_lel_muyin_zixinguan_20170401_phoneid_200
as
select a.phone from t_lel_muyin_zixingaun_20170401_phoneid a where a.phone not in (select phone from t_lel_touzilicai_zixingaun_20170401_phoneid_200)  order by rand() limit 200
3.
create table t_lel_car_zixinguan_20170401_phoneid_200
as
select a.phone from t_lel_car_zixinguan_20170401_phoneid a where a.phone not in 
(
select phone from t_lel_touzilicai_zixinguan_20170401_phoneid_200
union all
select phone from t_lel_muyin_zixinguan_20170401_phoneid_200
)  order by rand() limit 200
4.
create table t_lel_creditcard_zixinguan_20170401_phoneid_200
as
select a.phone from t_lel_zixinguan_creditcard_20170401 a where a.phone not in 
(
select phone from t_lel_touzilicai_zixinguan_20170401_phoneid_200
union all
select phone from t_lel_muyin_zixinguan_20170401_phoneid_200
union all
select phone from t_lel_car_zixinguan_20170401_phoneid_200
)  order by rand() limit 200
5.
create table t_lel_jiedai_zixinguan_20170401_phoneid_200
as
select a.phone from t_lel_zixinguan_creditcard_20170401 a where a.phone not in 
(
select phone from t_lel_touzilicai_zixinguan_20170401_phoneid_200
union all
select phone from t_lel_muyin_zixinguan_20170401_phoneid_200
union all
select phone from t_lel_car_zixinguan_20170401_phoneid_200
union all
select phone from t_lel_creditcard_zixinguan_20170401_phoneid_200
)  order by rand() limit 200
