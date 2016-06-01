--
--
-- SELECT
-- sum(case when split(t1.tloc,'\t')[0]= split(t2.location,'\t')[0] then 1 else 0 end ) ,
-- sum(1)
-- from
-- t_base_user_info_s t1  join  t_base_ec_tb_userinfo  t2 on t1.ds=20160310 and t1.tb_id=t2.uid ;
--
--
-- 20160418
-- 776612  185953723
--
--
--
-- 20160310
-- 2447757 185953723
--
-- select count(1) from t_base_user_info_s where    ds=20160418;


SELECT
*
from
t_base_user_info_s_tbuserinfo_t
where
tage>0   and

LENGTH(tgender)>0 and
LENGTH(tname)>0  and
LENGTH(alipay)>0 limit 100
;

--

create TABLE  t_base_user_info_s_tbuserinfo_t as
SELECT
tb_id,
case when LENGTH(max(tgender))<1 then null else max(tgender) end as tgender ,
case when max(tage) is null  then null else max(tage) end as tage ,
case when LENGTH(max(tname))<1 then null else max(tname) end as tname ,
case when LENGTH(max(tloc))<1 then null else max(tloc) end as tloc ,
case when LENGTH(max(alipay))<1 then null else max(alipay) end as alipay ,
case when LENGTH(max(buycnt))<1 then null else max(buycnt) end as buycnt ,
case when LENGTH(max(verify))<1 then null else max(verify) end as verify ,
case when LENGTH(max(regtime))<1 then null else max(regtime) end as regtime ,
case when LENGTH(max(nick))<1 then null else max(nick) end as nick
FROM
(
SELECT
tb_id,CAST ( cast(tgender as int) as String ) as tgender,tage,tname,
case when  t1.tloc is not null  then t1.tloc else t2.location end as  tloc,
case when alipay is not null then  alipay else "" end as alipay ,
case when buycnt is not null then  buycnt else "" end as buycnt ,
case when verify is not null then verify else "" end as verify ,
case when regtime is not null then  regtime else "" end as regtime ,
case when nick is not null then nick else "" end as nick
from t_base_user_info_s t1  left join  t_base_ec_tb_userinfo  t2
on t1.ds=20160310 and t1.tb_id=t2.uid and t2.ds=20160503

 )t group by  tb_id;

## t_base_user_info_s   20160310  µÄÊý¾Ý  391448818



create TABLE  t_base_user_info_s_tbuserinfo_t_20160418 as
SELECT
tb_id,
case when LENGTH(max(tgender))<1 then null else max(tgender) end as tgender ,
case when max(tage) is null  then null else max(tage) end as tage ,
case when LENGTH(max(tname))<1 then null else max(tname) end as tname ,
case when LENGTH(max(tloc))<1 then null else max(tloc) end as tloc ,
case when LENGTH(max(alipay))<1 then null else max(alipay) end as alipay ,
case when LENGTH(max(buycnt))<1 then null else max(buycnt) end as buycnt ,
case when LENGTH(max(verify))<1 then null else max(verify) end as verify ,
case when LENGTH(max(regtime))<1 then null else max(regtime) end as regtime ,
case when LENGTH(max(nick))<1 then null else max(nick) end as nick
FROM
(
SELECT
tb_id,CAST ( cast(tgender as int) as String ) as tgender,tage,tname,
case when  t2.location is not null  then t2.location else  t1.tloc end as  tloc,
case when alipay is not null then  alipay else "" end as alipay ,
case when buycnt is not null then  buycnt else "" end as buycnt ,
case when verify is not null then   verify else "" end as verify ,
case when regtime is not null then  regtime else "" end as regtime ,
case when nick is not null then nick else "" end as nick
from t_base_user_info_s t1  full join  t_base_ec_tb_userinfo  t2
on t1.ds=20160418 and t1.tb_id=t2.uid and t2.ds=20160530

 )t group by  tb_id;





create TABLE  t_base_user_info_s_tbuserinfo_t as
select  t1.*,prov as tel_prov,city as tel_city from t_base_user_info_s_tbuserinfo_t_20160418 t1 left join t_base_ec_loc t2  on t1.tb_id=t2.user_id;


select


from
(
select sum(day_sold ) day_soldf,sum(day_sold_price) totsold,t2.cat_id,t2.cat_name,substring(t1.ds,0,6) m,
from t_wrt_mask_daysale t1 join t_wrt_mask_iteminfo t2
on t1.item_id=t2.item_id and t2.ds='20160527'
group by cat_id ,cat_name  ,substring(t1.ds,0,6)

)t1
JOIN
(
select sum(day_sold ) day_soldf,sum(day_sold_price) totsold,t2.cat_id,t2.cat_name,substring(t1.ds,0,6) m
from t_wrt_mask_daysale t1 join t_wrt_mask_iteminfo t2
on t1.item_id=t2.item_id and t2.ds='20160527'
group by cat_id ,cat_name  ,substring(t1.ds,0,6)

)t2

on t1.cat_id =t2.cat_id and t1.m=t2.m




select level,count(1) from (select CAST(log2(price) as int) level from t_base_ec_item_dev_new WHERE  ds=20160530) t group by level