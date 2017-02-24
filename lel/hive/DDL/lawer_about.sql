CREATE TABLE  if not exists wl_base.t_base_lawer(
phone String COMMENT '电话号码',
company String COMMENT '公司或者个人',
source String COMMENT '信息来源'
)
COMMENT '律师相关'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;



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
