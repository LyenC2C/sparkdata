CREATE TABLE  if not exists wl_base.t_base_phone_sougou_bankname(
phone_number String COMMENT '电话号码',
name String COMMENT '银行名称'
)
COMMENT '搜狗搜索银行号码及名称'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/sougou_bankname_search" overwrite into table wl_base.t_base_phone_sougou_bankname partition(ds=20170216)
