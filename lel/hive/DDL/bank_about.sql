CREATE TABLE  if not exists wl_base.t_base_bank(
phone String COMMENT '电话号码',
company String COMMENT '银行名称',
source String COMMENT '信息来源'
)
COMMENT '银行相关'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;



insert into table wl_base.t_base_bank partition(ds=20170224)
select distinct(phone),bank,"bankinfo" as source
from
(select regexp_replace(phone_number,'-','') as phone, name as bank from wl_base.t_base_bankinfo
union all
select phone,name as bank from wl_base.t_base_phone_sougou_bankname) a