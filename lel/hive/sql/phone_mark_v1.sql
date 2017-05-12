extact:
->keywords->'贷|贷款|担保|典当|抵押|质押|融资租赁'

58_daikuan:
create table if not exists wlcredit.t_base_credit_58_info_fraud_filtered
as
select 
decrypted_tel,nickname,
case when regexp_extract(keywords,'贷款|纠纷',0)='纠纷' 
then '律师' else '贷款' end as cate 
from t_base_credit_58_info_fraud_1208 
where keywords regexp '贷款|纠纷'


sougou_400_800_daikuan:

create table if not exists wl_base.t_base_phone_sougou_400_800_filtered
as
select 
phone,tag as company
from wl_base.t_base_phone_sougou_400_800 
where tag <> '' 
and tag is not null 
and amount=0 
and tag regexp '贷|贷款|担保|典当|抵押|质押|融资租赁'




souhou_huangye88_daikuan:

set hive.merge.mapfiles=true;
create table if not exists t_base_phone_huangye88_daikuan_filtered
as
select t1.phone as phone,case when t2.company ='' then t1.contact else t2.company end as company
from
(select 
phone,company,contact
from t_base_phone_huangye88_daikuan ) t1
join 
(select 
phone,company
from t_base_phone_huangye88_daikuan ) t2
on t1.phone=t2.phone

souhou_huangye88_userinfo_daikuan:
create table t_base_phone_huangye88_userinfo_daikuan as 
select phone,company from t_base_phone_huangye88_userinfo where company regexp '贷|贷款|担保|典当|抵押|质押|融资租赁'

dianhuabang_daikuan:

set hive.merge.mapfiles=true;
drop table if exists t_base_phone_dianhuabang_daikuan;
create table if not exists t_base_phone_dianhuabang_daikuan
as 
select phone,name as company from t_base_phone_dianhuabang where name regexp '贷|贷款|担保|典当|抵押|质押|融资租赁'


final:
insert into table wl_base.t_base_daikuan
select distinct(regexp_replace(phone,'-','')) as phone,company,source from
(
select *,"sougou400_800" as source from wl_base.t_base_phone_sougou_400_800_daikuan
union all
select *,"dianhuabang" as source from wl_base.t_base_phone_dianhuabang_daikuan
union all
select *,"huangye88" as source from wl_base.t_base_phone_huangye88_daikuan_filtered
union all
select *,"huangye88" as source from wl_base.t_base_phone_huangye88_userinfo_daikuan
union all
select phone,company,"58" as source from wlcredit.t_base_credit_58_info_fraud_filtered where cate='贷款'
) a



lawer:

1.	265
create table if not exists wl_base.t_base_phone_sougou_400_800_lawer
as
select 
phone,tag as company
from wl_base.t_base_phone_sougou_400_800 
where tag <> '' 
and tag is not null 
and amount=0 
and tag regexp '律师'
2.	12141
select * from t_base_phone_find_lawer_filtered
3.	3172
create table if not exists t_base_phone_dianhuabang_lawer
as 
select phone,name as company from t_base_phone_dianhuabang where name regexp '律师'
4.	785
souhou_huangye88_userinfo_daikuan:
create table t_base_phone_huangye88_userinfo_lawer as 
select phone,company from t_base_phone_huangye88_userinfo where company regexp '律师'
5.	11285
select phone,company from wlcredit.t_base_credit_58_info_fraud_filtered where cate='律师'


final:
insert into table wl_base.t_base_lawer partition(ds=20170224)
select distinct(regexp_replace(phone,'-','')) as phone,company,source
from
(select *,"sougou400_800" as source from wl_base.t_base_phone_sougou_400_800_lawer
union all
select *,"findlawer" as source from wl_base.t_base_phone_find_lawer_filtered
union all
select *,"dianhuabang" as source from wl_base.t_base_phone_dianhuabang_lawer
union all
select *,"huangye88" as source from wl_base.t_base_phone_huangye88_userinfo_lawer
union all
select phone,company,"58" as source from wlcredit.t_base_credit_58_info_fraud_filtered where cate='律师') a


bank:
insert into table wl_base.t_base_bank partition(ds=20170224)
select distinct(phone),bank,"bankinfo" as source
from
(select regexp_replace(phone_number,'-','') as phone, name as bank,from wl_base.t_base_bankinfo
union all
select phone,name as bank from wl_base.t_base_phone_sougou_bankname) a 

creditbank:




product_v1:

create table wl_base.t_base_blcd_multi_product_v1
as 
select phone,belong,cate,source
from
(select phone,bank as belong,"银行" as cate,source from wl_base.t_base_bank
union all
select phone,company as belong,"律师" as cate,source from wl_base.t_base_lawer
union all
select phone,bank as belong,"信用卡" as cate,source from wl_base.t_base_creditbank
union all
select phone,company as belong,"贷款中介" as cate,source from wl_base.t_base_daikuan) a