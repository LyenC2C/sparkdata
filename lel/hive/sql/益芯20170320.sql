益芯20170320

1.
create table wl_service.t_lel_yixin_shandian_20170320
as
select phone,platform from wl_base.t_base_yixin_daichang where ds='20170321' and flag='True' and platform regexp '闪电' 

select a.* from wl_service.t_lel_yixin_shandian_20170320 a where a.phone not in (select phone from t_lel_yixin_phone_platform_given_20170308) limit 800

2.
create table wl_service.t_lel_yixin_budingxiaodai_20170320
as
select phone,platform from wl_base.t_base_yixin_daichang where ds='20170321' and flag='True' and platform regexp '布丁' 

select a.* from wl_service.t_lel_yixin_budingxiaodai_20170320 a where a.phone not in (select phone from wl_service.t_lel_yixin_shandian_20170320
union all 
select phone from t_lel_yixin_phone_platform_given_20170308) limit 23200
3.
drop table wl_service.t_lel_yixin_shanyin_20170320;
create table wl_service.t_lel_yixin_shanyin_20170320
as
select phone,platform from wl_base.t_base_yixin_daichang where ds='20170321' and flag='True' and platform regexp '闪银' 

select a.* from wl_service.t_lel_yixin_shanyin_20170320 a where a.phone not in (select phone from wl_service.t_lel_yixin_shandian_20170320
union all select phone from wl_service.t_lel_yixin_budingxiaodai_20170320 union all select phone from t_lel_yixin_phone_platform_given_20170308) limit 76000
4.
create table wl_service.t_lel_yixin_kaka_20170320
as
select phone,platform from wl_base.t_base_yixin_daichang where ds='20170321' and flag='True' and platform regexp '卡卡' 

select a.* from wl_service.t_lel_yixin_kaka_20170320 a where a.phone not in (select phone from wl_service.t_lel_yixin_shandian_20170320
union all select phone from wl_service.t_lel_yixin_budingxiaodai_20170320 union all select phone from t_lel_yixin_phone_platform_given_20170308
union all select phone from wl_service.t_lel_yixin_shanyin_20170320) limit 300000


益芯20170327

select a.* from wl_service.t_lel_yixin_kaka_20170320 a where a.phone not in ( select phone from t_lel_yixin_phone_platform_given_20170308) limit 300000

益芯20170405
select a.* from wl_service.t_lel_yixin_kaka_20170320 a where a.phone not in ( select phone from t_lel_yixin_phone_platform_given_20170308) limit 300000

益芯20170414
select a.* from wl_service.t_lel_yixin_kaka_20170320 a where a.phone not in ( select phone from t_lel_yixin_phone_platform_given_20170308) limit 300000

