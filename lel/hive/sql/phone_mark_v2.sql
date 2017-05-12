phone_mark_v1

1.bankinfo
create table wl_service.t_base_bankinfo_v1_filtered
as
select phone,acode,phone_num,name as mark,"银行" as cate,"bankinfo" as source from t_base_bankinfo_v1 
where (length(acode)  >= 3 and length(acode)  <= 7 or acode='' ) and (length(phone_num) >= 5 and length(phone_num) =< 10) 
2.findlawer
create table wl_service.t_base_phone_find_lawer_v1_filtered
as
select 
phone,
acode,
phone_num,
concat_ws("->",lawer_name,practice_company) as mark,"律师" as cate,"findlawer" as source 
from t_base_phone_find_lawer_v1 where phone_num <> ''

reset:
create table wl_service.t_base_phone_find_lawer_v1_filtered
as
select 
regexp_replace(phone,'-','') as phone,
case when phone not regexp '-' and phone not regexp '^0' then ''
when phone not regexp '-' and phone regexp '^0' and phone regexp '^01|^02' then substr(phone,1,3)
when phone not regexp '-' and phone regexp '^0' and phone not regexp '^01|^02' then substr(phone,1,4)
when phone regexp '-' and phone not regexp '0' then ''
when phone regexp '-' and phone regexp '^0' then split(phone,'-')[0]
when phone regexp '^400|^800' then ''
end as acode,
case when phone not regexp '-' and phone not regexp '^0' then phone
when phone not regexp '-' and phone regexp '^0' and phone regexp '^01|^02' then substr(phone,4,length(phone))
when phone not regexp '-' and phone regexp '^0' and phone not regexp '^01|^02' then substr(phone,5,length(phone))
when phone regexp '-' and phone not regexp '^0' then regexp_replace(phone,'-','')
when phone regexp '-' and phone regexp '^0' then split(phone,'-')[1]
when phone regexp '^400|^800' then regexp_replace(phone,'-','')
end as phone_num,
concat_ws("->",lawer_name,practice_company) as mark,"律师" as cate,"findlawer" as source 
from t_base_phone_find_lawer_v1 where phone_num <> ''


3.dianhuabang
create table wl_service.t_base_phone_dianhuabang_simple_filtered
as
select phone,acode,phone_num,name as mark,"" as cate,"dianhuabang" as source from t_base_phone_dianhuabang_simple 
where (length(acode)  >= 3 and length(acode)  <= 7 or acode='' ) and (length(phone_num) >= 5 and length(phone_num) <= 10) 
4.sougou400_800
create table wl_service.t_base_phone_sougou_400_800_filtered
as
select word as phone,'' as acode,word as phone_num,tag as mark,'' as cate,'sougou400_800' as source from t_base_phone_sougou_400_800 where tag <> '' and tag is not null
5.sougou_bankname
create table wl_service.t_base_phone_sougou_bankname_filtered
as
select
regexp_extract(phone_number,'\\d+',0) as phone,
case when phone_number regexp '^400|^800' then ''
when length(regexp_extract(phone_number,'\\d+',0)) =5 then ''
when phone_number regexp '^01|^02' then substr(regexp_extract(phone_number,'\\d+',0),1,3)
when phone_number not regexp '^01|^02' and length(regexp_extract(phone_number,'\\d+',0)) <> 5  then substr(regexp_extract(phone_number,'\\d+',0),1,4)
end as acode,
case when phone_number regexp '^01|^02' then substr(regexp_extract(phone_number,'\\d+',0),4,length(regexp_extract(phone_number,'\\d+',0)))
when phone_number regexp '^400|^800' then regexp_extract(phone_number,'\\d+',0)
when length(regexp_extract(phone_number,'\\d+',0)) =5 then regexp_extract(phone_number,'\\d+',0)
when phone_number not regexp '^01|^02' and  length(regexp_extract(phone_number,'\\d+',0)) <> 5  then substr(regexp_extract(phone_number,'\\d+',0),5,length(regexp_extract(phone_number,'\\d+',0)))
end as phone_num,
name as mark,
'' as cate,
'bankinfo' as source
from t_base_phone_sougou_bankname

6.creditbank
create table wl_service.t_base_bankcredit_for_tag_filtered
as
select phone,'' as acode,phone as phone_num,bankname as mark,'信用卡' as cate,'manually' as source from t_base_bankcredit_for_tag
7.huangye88_userinfo
create table wl_service.t_base_phone_huangye88_userinfo_v1_filtered
as
select phone,acode,phone_num,company as mark,'' as cate,"huangye88" as source from t_base_phone_huangye88_userinfo_v1 where company <> '' and length(phone_num) <=11
8.huangye88_daikuan
create table wl_service.t_base_phone_huangye88_daikuan_filtered
as
select phone,'' as acode,phone as phone_num,concat_ws("->",contact,company) as mark,"贷款中介" as cate,'huangye88' as source  from t_base_phone_huangye88_daikuan where company <> ''
9.58_userinfo
drop table wl_service.t_base_credit_58_info_v1_filtered;
create table wl_service.t_base_credit_58_info_v1_filtered
as
select
regexp_replace(decrypted_tel,'-','') as phone,
case when decrypted_tel not regexp '-' and decrypted_tel not regexp '^0' then ''
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel regexp '^01|^02' then substr(decrypted_tel,1,3)
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel not regexp '^01|^02' then substr(decrypted_tel,1,4)
when decrypted_tel regexp '-' and decrypted_tel not regexp '0' then ''
when decrypted_tel regexp '-' and decrypted_tel regexp '^0' then split(decrypted_tel,'-')[0]
when decrypted_tel regexp '^400|^800' then ''
end as acode,
case when decrypted_tel not regexp '-' and decrypted_tel not regexp '^0' then decrypted_tel
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel regexp '^01|^02' then substr(decrypted_tel,4,length(decrypted_tel))
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel not regexp '^01|^02' then substr(decrypted_tel,5,length(decrypted_tel))
when decrypted_tel regexp '-' and decrypted_tel not regexp '^0' then regexp_replace(decrypted_tel,'-','')
when decrypted_tel regexp '-' and decrypted_tel regexp '^0' then split(decrypted_tel,'-')[1]
when decrypted_tel regexp '^400|^800' then regexp_replace(decrypted_tel,'-','')
end as phone_num,
nickname as mark,
'' as cate,
"58" as source
from t_base_credit_58_info where ds = '20161208'
10.58fraud
drop table wl_service.t_base_credit_58_info_fraud_v1_1208filtered;
create table wl_service.t_base_credit_58_info_fraud_v1_1208filtered
as
select
regexp_replace(decrypted_tel,'-','') as phone,
case when decrypted_tel not regexp '-' and decrypted_tel not regexp '^0' then ''
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel regexp '^01|^02' then substr(decrypted_tel,1,3)
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel not regexp '^01|^02' then substr(decrypted_tel,1,4)
when decrypted_tel regexp '-' and decrypted_tel not regexp '0' then ''
when decrypted_tel regexp '-' and decrypted_tel regexp '^0' then split(decrypted_tel,'-')[0]
when decrypted_tel regexp '^400|^800' then ''
end as acode,
case when decrypted_tel not regexp '-' and decrypted_tel not regexp '^0' then decrypted_tel
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel regexp '^01|^02' then substr(decrypted_tel,4,length(decrypted_tel))
when decrypted_tel not regexp '-' and decrypted_tel regexp '^0' and decrypted_tel not regexp '^01|^02' then substr(decrypted_tel,5,length(decrypted_tel))
when decrypted_tel regexp '-' and decrypted_tel not regexp '^0' then regexp_replace(decrypted_tel,'-','')
when decrypted_tel regexp '-' and decrypted_tel regexp '^0' then split(decrypted_tel,'-')[1]
when decrypted_tel regexp '^400|^800' then regexp_replace(decrypted_tel,'-','')
end as phone_num,
nickname as mark,
'' as cate,
"58" as source
from wl_base.t_base_credit_58_info_fraud_1208filtered 


union:
drop table wl_service.t_lel_phone_mark_v1;
create table wl_service.t_lel_phone_mark_v1
as
select * from t_base_bankcredit_for_tag_filtered
union all
select * from t_base_bankinfo_v1_filtered
union all
select * from t_base_credit_58_info_v1_filtered
union all
select * from t_base_phone_dianhuabang_simple_filtered
union all
select * from t_base_phone_find_lawer_v1_filtered
union all
select * from t_base_phone_huangye88_daikuan_filtered
union all
select * from t_base_phone_huangye88_userinfo_v1_filtered
union all
select * from t_base_phone_sougou_400_800_filtered
union all
select * from t_base_phone_sougou_bankname_filtered
union all
select * from t_base_credit_58_info_fraud_v1_1208filtered


drop table t_lel_phone_mark_v1_makeup;
create table t_lel_phone_mark_v1_makeup
as
select distinct a.phone,a.acode,a.phone_num,a.mark,
case when a.cate = '' then b.cate else a.cate
end as cate,
a.source
from
(select * from t_lel_phone_mark_v1 where length(phone) >=5 and length(phone_num) >=5)a
left join 
(select * from t_lel_blcd_multi_product_v1 where length(phone) >=5 and length(phone_num) >=5)b
on a.phone = b.phone;

drop table t_lel_phone_mark_samesource_takeone;
create table t_lel_phone_mark_samesource_takeone
as
SELECT phone,acode,phone_num,collect_set(mark)[0] as mark,cate,source  from  t_lel_phone_mark_v1_makeup  group by source,phone,acode,phone_num,cate;
drop table t_lel_phone_mark_samesource_takeone_with_priority;
create table t_lel_phone_mark_samesource_takeone_with_priority
as 
select *,
case when source = 'manually' then 7 
when source = 'sougou400_800' then 6
when source = 'bankinfo' then 5
when source = 'findlawer' then 4
when source = 'huangye88' then 3
when source = 'dianhuabang' then 2
when source = '58' then 1 end as priority
from t_lel_phone_mark_samesource_takeone;

drop table t_lel_phone_mark_samesource_takeone_with_priority_dis;
create table t_lel_phone_mark_samesource_takeone_with_priority_dis
as
SELECT phone,acode,phone_num,mark,cate,max(priority) as p  from  t_lel_phone_mark_samesource_takeone_with_priority group by phone,acode,phone_num,mark,cate;

drop table t_lel_phone_mark_v2;
create table t_lel_phone_mark_v2
as
select phone,acode,phone_num,collect_set(mark)[0] as mark,case when cate is null then '' end as cate from  t_lel_phone_mark_samesource_takeone_with_priority_dis group by phone,acode,phone_num,cate;



stat:

select a.phone, a.acode, a.phone_num, collect_set(a.mark) as marks, a.cate 
from
(select * from t_lel_phone_mark_v1_makeup where cate = '贷款中介')a
group by phone, acode, phone_num,cate, source having size(marks) >1

select * from 
t_lel_phone_mark_v2 
where phone_num in ('95574', '95511','95508','95508','95566','95566','95594','95588','95512','95580','10658678','1008611','106907792032','106571014806','106906612505 ','106907300390') 

to_develop