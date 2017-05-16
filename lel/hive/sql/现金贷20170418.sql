现金贷20170418

create external  table 
	t_lel_xianjindai_3scene_20170418(phone string,tbid string) location '/trans/lel/xianjindai/'

create table t_lel_xianjindai_3scene_20170418_fitered
as
select a.phone,a.tbid from 
(select phone,substr(phone,1,7) as prefix,tbid from t_lel_xianjindai_3scene_20170418)a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix 

近期结婚客群:
create table t_lel_xianjindai_jiehun_20170417_cd_all
as
select b.phone,a.keywords,a.num_keyword
from
(select * from t_lel_xianjindai_jiehun_20170417_cd)a
join 
(select * from t_lel_xianjindai_3scene_20170418_fitered)b
on a.user_id = b.tbid
-->49593

近期装修消费场景客群:	
create table t_lel_xianjindai_zhuangxiu_20170417_cd_all
as
select b.phone,a.keywords,a.num_keyword
from
(select * from t_lel_xianjindai_zhuangxiu_20170417_cd)a
join 
(select * from t_lel_xianjindai_3scene_20170418_fitered)b
on a.user_id = b.tbid
-->51186
近期出境游客群:
create table t_lel_xianjindai_travel_20170417_cd_all
as
select b.phone,a.keywords,a.num_keyword
from
(select * from t_lel_xianjindai_travel_20170417_cd)a
join 
(select * from t_lel_xianjindai_3scene_20170418_fitered)b
on a.user_id = b.tbid
-->3380

优质贷款客群:
t_lel_xjd_platform_credit_bank_cd
-->2250
中等资质客群
t_lel_xjd_platform_credit_daichang_cd
-->146482
普通资质客群借贷平台数<=3  
t_lel_xianjindai_le3
-->7615
普通资质客群借贷平台数>=4
t_lel_xianjindai_ge4
-->20023

#######################################
1.近期出境游客群                         
2.近期装修消费场景客群                    
3.近期结婚客群                          
divide by regist multiplatform or not:
#######################################
create table t_lel_xianjindai_zhuangxiu_20170417_cd_all_regist
as
select a.phone,b.keywords,b.c
from 
(select phone from wl_service.t_lel_xianjindai_zhuangxiu_20170417_cd_all)a
join
(select phone,concat_ws("|",collect_set(platform)) as keywords,count(1) as c  from wl_base.t_base_multiplatform where ds = '20170417' and flag='True' group by phone)b
on a.phone = b.phone

create table t_lel_xianjindai_zhuangxiu_20170417_cd_all_noregist
as
select a.phone from t_lel_xianjindai_zhuangxiu_20170417_cd_all a
where a.phone not in (select phone  from wl_base.t_base_multiplatform where ds = '20170417' and flag='True')




create table t_lel_xianjindai_jiehun_20170417_cd_all_regist
as
select a.phone,b.keywords,b.c
from 
(select phone from wl_service.t_lel_xianjindai_jiehun_20170417_cd_all)a
join
(select phone,concat_ws("|",collect_set(platform)) as keywords,count(1) as c  from wl_base.t_base_multiplatform where ds = '20170417' and flag='True' group by phone)b
on a.phone = b.phone;
create table t_lel_xianjindai_jiehun_20170417_cd_all_noregist
as
select a.phone from t_lel_xianjindai_jiehun_20170417_cd_all a
where a.phone not in 
(select phone  from wl_base.t_base_multiplatform where ds = '20170417' and flag='True');
create table t_lel_xianjindai_travel_20170417_cd_all_regist
as
select a.phone,b.keywords,b.c
from 
(select phone from wl_service.t_lel_xianjindai_travel_20170417_cd_all)a
join
(select phone,concat_ws("|",collect_set(platform)) as keywords,count(1) as c  from wl_base.t_base_multiplatform where ds = '20170417' and flag='True' group by phone)b
on a.phone = b.phone;
create table t_lel_xianjindai_travel_20170417_cd_all_noregist
as
select a.phone from t_lel_xianjindai_travel_20170417_cd_all a
where a.phone not in 
(select phone  from wl_base.t_base_multiplatform where ds = '20170417' and flag='True');