现金贷20170417



check:
create table wl_service.t_lel_xianjindai_jiehun_20170417
as
SELECT a,user_id,a.keywords from
(select user_id,group_concat(regexp_extract(title,"结婚请帖|喜糖盒|婚纱|敬酒服|喜字贴",0),"|") as keywords from t_base_ec_record_dev_new where ds = 'true' and title regexp '结婚请帖|喜糖盒|婚纱|敬酒服|喜字贴' and cast(dsn) > 20170102 
group by user_id)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id = b.tb_id





wl_service.t_lzh_zhuangxiu      
-->69140502
wl_service.t_lzh_jiehun         jiehun:2177
-->2576456
wl_service.t_lzh_lvyou_170414   travel:1793
-->214948

select  count(a.phone) from
(select phone,substr(phone,1,7) as prefix,count(1) as c
from t_base_multiplatform
where platform regexp'布丁小贷|贷上钱|豆豆花|给你花|简单借款|九秒贷|屌丝贷|钱米应急钱包|缺钱么|星星钱袋|现金速递|小葱钱包|月光侠|指尖贷|趣分期' and ds='20170417' and flag='True' group by phone,substr(phone,1,7) having c <4) a
left semi join
(select prefix from t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix




drop table  wl_service.t_lel_xjd_platform_credit_bank;
create table wl_service.t_lel_xjd_platform_credit_bank
as
select a.phone from 
(select phone
from t_base_multiplatform
where platform regexp'名校贷|万达贷|招联金融' and ds='20170401' and flag='True')a
join
(select phone,count(1) as platforms
from t_base_multiplatform
where  ds='20170401' and flag='True' group by phone having platforms <=4) b
on a.phone = b.phone
join
(select phone from t_base_credit_bank where ds='20170306' and flag = 'True')c
on a.phone =c.phone



create table wl_service.t_lel_xjd_platform_credit_bank_cd
as
select distinct a.phone from
(select phone,substr(phone,1,7) as prefix  from wl_service.t_lel_xjd_platform_credit_bank)a
join
(select prefix,city from t_base_mobile_loc where city regexp '成都')b
on a.prefix=b.prefix






product:

优质贷款客群:
1、信用卡用户
2、注册名校贷|万达贷|招联金融其中之一平台用户
3、借贷平台注册数量≤4的用户
需要同时满足1、2、3项条件用户群

drop table  wl_service.t_lel_xjd_platform_credit_bank;
create table wl_service.t_lel_xjd_platform_credit_bank
as
select a.phone from 
(select phone
from t_base_multiplatform
where platform regexp'名校贷|万达贷|招联金融' and ds='20170401' and flag='True')a
join
(select phone,count(1) as platforms
from t_base_multiplatform
where  ds='20170401' and flag='True' group by phone having platforms <=4) b
on a.phone = b.phone
join
(select phone from t_base_credit_bank where ds='20170306' and flag = 'True')c
on a.phone =c.phone

create table wl_service.t_lel_xjd_platform_credit_bank_cd
as
select distinct a.phone from
(select phone,substr(phone,1,7) as prefix  from wl_service.t_lel_xjd_platform_credit_bank)a
join
(select prefix,city from t_base_mobile_loc where city regexp '成都')b
on a.prefix=b.prefix

select count(1) from wl_service.t_lel_xjd_platform_credit_bank_cd
-->2550

中等资质客群:
1、信用卡用户
2、信用卡代偿平台注册客群


create table wl_service.t_lel_xjd_platform_credit_daichang
as
select distinct t1.phone from
(select phone from t_base_credit_bank
where ds='20170306' and flag='True')t1
join
(select phone from t_base_yixin_daichang where ds='20170320' and flag='True' and platform regexp'卡卡贷|支付通|天天付'
)t2
on a.phone = b.phone

select a.phone from 
(select phone,substr(phone,1,7) as prefix from wl_service.t_lel_xjd_platform_credit_daichang) a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix

-->146482
普通资质客群	
1、对应普通资质借贷平台注册客群—见附件
2、借贷平台数≤3
create table wl_service.t_lel_xianjindai_le3
as
select m.phone from
(select phone from
(select phone,substr(phone,1,7) as prefix
from wl_base.t_base_multiplatform
where platform regexp'布丁小贷|贷上钱|豆豆花|给你花|简单借款|九秒贷|屌丝贷|钱米应急钱包|缺钱么|星星钱袋|现金速递|小葱钱包|月光侠|指尖贷|趣分期' and ds='20170417' and flag='True' group by phone,substr(phone,1,7)) a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix) m
where m.phone
in
(select phone from
(select phone,substr(phone,1,7) as prefix,count(1) as c
from wl_base.t_base_multiplatform
where ds='20170417' and flag='True' group by phone,substr(phone,1,7) having c<=3) a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix)

1、对应普通资质借贷平台注册客群—见附件
2、借贷平台数≥4
create table wl_service.t_lel_xianjindai_ge4
as
select m.phone from
(select phone from
(select phone,substr(phone,1,7) as prefix
from wl_base.t_base_multiplatform
where platform regexp'布丁小贷|贷上钱|豆豆花|给你花|简单借款|九秒贷|屌丝贷|钱米应急钱包|缺钱么|星星钱袋|现金速递|小葱钱包|月光侠|指尖贷|趣分期' and ds='20170417' and flag='True' group by phone,substr(phone,1,7)) a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix) m
where m.phone
in
(select phone from
(select phone,substr(phone,1,7) as prefix,count(1) as c
from wl_base.t_base_multiplatform
where ds='20170417' and flag='True' group by phone,substr(phone,1,7) having c>=4) a
left semi join
(select prefix from wl_base.t_base_mobile_loc where city regexp '成都')b
on a.prefix = b.prefix)



近期结婚客群:
create table wl_service.t_lel_xianjindai_jiehun_20170417_cd
as
SELECT a.user_id,a.keywords,a.num_keyword from
(select user_id,concat_ws("|",collect_set(regexp_extract(title,"结婚请帖|喜糖盒|婚纱|敬酒服|喜字贴",0))) as keywords,size(collect_set(regexp_extract(title,"结婚请帖|喜糖盒|婚纱|敬酒服|喜字贴",0))) as num_keyword from t_base_ec_record_dev_new where ds = 'true' and title regexp '结婚请帖|喜糖盒|婚纱|敬酒服|喜字贴' and cast(dsn as int) > 20170102 
group by user_id)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id = b.tb_id

create table wl_service.t_lel_xianjindai_jiehun_20170417_cd_res
as
select a.phone_number,b.keywords from 
(select phone_number,tid from t_lzh_jiehun15wtel)a
join
(select user_id,keywords from t_lel_xianjindai_jiehun_20170417_cd)b
on a.tid = b.user_id
where length(a.phone_number)=11


近期装修消费场景客群:
create table wl_service.t_lel_xianjindai_zhuangxiu_20170417_cd
as
SELECT a.user_id,a.keywords,a.num_keyword from
(select user_id,concat_ws("|",collect_set(regexp_extract(title,"装修|瓷砖|地板|全屋定制",0))) as keywords,size(collect_set(regexp_extract(title,"装修|瓷砖|地板|全屋定制",0))) as num_keyword from t_base_ec_record_dev_new where ds = 'true' and title regexp '装修|瓷砖|地板|全屋定制' and cast(dsn as int) > 20170302 
group by user_id)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id = b.tb_id

drop table  wl_service.t_lel_xianjindai_zhuangxiu_20170417_cd_res;
create table wl_service.t_lel_xianjindai_zhuangxiu_20170417_cd_res
as
select a.phone,b.keywords from 
(select phone,id from wl_service.t_lel_xianjindai_zhuangxiu_phone_20170417_cd)a
join
(select user_id,keywords from t_lel_xianjindai_zhuangxiu_20170417_cd)b
on a.id = b.user_id
where length(a.phone)=11

近期出境游客群:
create table wl_service.t_lel_xianjindai_travel_20170417_cd
as
SELECT a.user_id,a.keywords,a.num_keyword from
(select user_id,concat_ws("|",collect_set(regexp_extract(title,"泰国|日本|韩国|越南|马来西亚|马尔代夫|巴厘岛",0))) as keywords,size(collect_set(regexp_extract(title,"泰国|日本|韩国|越南|马来西亚|马尔代夫|巴厘岛",0))) as num_keyword from t_base_ec_record_dev_new where ds = 'true' and root_cat_id='50025707' and title regexp '泰国|日本|韩国|越南|马来西亚|马尔代夫|巴厘岛' and cast(dsn as int) > 20170302 
group by user_id)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on a.user_id = b.tb_id

create table wl_service.t_lel_xianjindai_travel_20170417_cd_res
as
select a.phone_number,b.keywords from 
(select phone_number,tid from wl_service.t_lzh_lvyou15wtel)a
join
(select user_id,keywords from t_lel_xianjindai_travel_20170417_cd)b
on a.tid = b.user_id
where length(a.phone_number)=11

product_final:
1.
create table t_lel_xjd_platform_credit_bank_cd_2k
as
select phone from t_lel_xjd_platform_credit_bank_cd limit 2000


2.
create table t_lel_xjd_platform_credit_daichang_2k
as
select a.phone from t_lel_xjd_platform_credit_daichang a where a.phone not in (select phone from t_lel_xjd_platform_credit_bank_cd_2k) limit 2000

3.1
create table t_lel_xianjindai_platform_le3_1k
as
select a.phone from t_lel_xianjindai_le3 a where a.phone not in (
select phone from t_lel_xjd_platform_credit_bank_cd_2k
union all
select phone from t_lel_xjd_platform_credit_daichang_2k
union all
select phone from t_lel_xianjindai_jiehun_20170417_cd_res_1k
union all
select phone from t_lel_xianjindai_zhuangxiu_20170417_cd_res_1k
union all
select phone from t_lel_xianjindai_travel_20170417_cd_res_1k
) limit 1000
3.2
create table t_lel_xianjindai_platform_ge4_1k
as
select a.phone from t_lel_xianjindai_ge4 a where a.phone not in (
select phone from t_lel_xjd_platform_credit_bank_cd_2k
union all
select phone from t_lel_xjd_platform_credit_daichang_2k
union all
select phone from t_lel_xianjindai_jiehun_20170417_cd_res_1k
union all
select phone from t_lel_xianjindai_zhuangxiu_20170417_cd_res_1k
union all
select phone from t_lel_xianjindai_travel_20170417_cd_res_1k
union all
select phone from t_lel_xianjindai_platform_le3_1k
) limit 1000

select count(1) from t_lel_xianjindai_platform_ge4_1k
4.
create table t_lel_xianjindai_jiehun_20170417_cd_res_1k
as
select a.phone_number as phone,a.keywords from t_lel_xianjindai_jiehun_20170417_cd_res a
where a.phone_number not in
(
select phone from t_lel_xjd_platform_credit_bank_cd_2k
union all
select phone from t_lel_xjd_platform_credit_daichang_2k
union all
select phone from t_lel_xjd_platform_jiedai_res_1k
) limit 1000
5.
create table t_lel_xianjindai_zhuangxiu_20170417_cd_res_1k
as
select a.phone,a.keywords from t_lel_xianjindai_zhuangxiu_20170417_cd_res a
where a.phone not in
(
select phone from t_lel_xjd_platform_credit_bank_cd_2k
union all
select phone from t_lel_xjd_platform_credit_daichang_2k
union all
select phone from t_lel_xjd_platform_jiedai_res_1k
union all
select phone from t_lel_xianjindai_jiehun_20170417_cd_res_1k
) limit 1000
6.
create table t_lel_xianjindai_travel_20170417_cd_res_1k
as
select a.phone_number as phone,a.keywords from t_lel_xianjindai_travel_20170417_cd_res a
where a.phone_number not in
(
select phone from t_lel_xjd_platform_credit_bank_cd_2k
union all
select phone from t_lel_xjd_platform_credit_daichang_2k
union all
select phone from t_lel_xjd_platform_jiedai_res_1k
union all
select phone from t_lel_xianjindai_jiehun_20170417_cd_res_1k
union all
select phone from t_lel_xianjindai_zhuangxiu_20170417_cd_res_1k
) limit 1000



