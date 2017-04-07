本周欣颜的需求为以下：
数量：500
近半年购买过关键词“美白 牙、电动牙刷、牙线、漱口水、黄牙”其中之一的客群
地区：成都
请与之前给出过的牙美相关电话去重处理。

输出字段：号码  姓名  性别  关键词

create table if not exists wlservice.t_lel_ec_xinyan_20161227_cd_igk as 
select t2.id as id,t1.gender as gender,t2.keyword as keyword from 
(select tb_id,xianyu_gender as gender from wlbase_dev.t_base_user_profile_telindex where  tb_location regexp '成都') t1
join
(select user_id as id,regexp_extract(title,'美白牙|电动牙刷|漱口水|黄牙|牙线',0) as keyword
 from wl_base.t_base_ec_record_dev_new 
 where
 (cast(dsn as int) 
 between  cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) - 600
 and cast(from_unixtime(unix_timestamp(),'yyyyMMdd') as int) )
 and  title regexp '美白牙|电动牙刷|漱口水|黄牙|牙线'
 ) t2
on t1.tb_id = t2.id; 


1.欣颜 

数量：500

近半年购买过关键词“美白 牙、电动牙刷、牙线、漱口水、黄牙”其中之一的客群
	
location: cs220  table:wlservice.t_lel_ec_xinyan_20161227_cd_igk

2.宜芯信用卡代偿需求提取 
location: cs100 table:wl_service.t_lel_weiboid_fansOf_zsjtxy_credit



