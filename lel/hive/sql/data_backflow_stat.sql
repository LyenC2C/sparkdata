data_backflow_stat.sql


qianlong:
create table wl_service.t_lel_qianlong_shoujidai_20170417
as
SELECT params_less["enc_m"] as phone FROM t_base_datamart_backflow where app_key in ("1897259559","2046796195") and  api_type in ("address_getbymobile","tel")


create table wl_service.t_lel_qianlong_shoujidai_20170417_pc2
as
select a.phone,count(1) as c1 FROM 
(select phone from wl_base.t_base_multiplatform where ds = '20170401' and flag= 'True')a
left semi join 
(select distinct(phone) as phone from wl_service.t_lel_qianlong_shoujidai_20170417)b
on a.phone = b.phone
group by phone
having c1 <=4
count--5087

select count(distinct a.phone) from
(select phone,substr(phone,1,7) as prefix  from t_lel_qianlong_shoujidai_20170417_pc2 )a
join
(select prefix from t_base_mobile_loc where city regexp '成都')b
on a.prefix=b.prefix

count_cd--212




select count(1) from wl_service.t_lel_qianlong_shoujidai_20170417_pc2