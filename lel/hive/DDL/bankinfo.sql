CREATE TABLE  if not exists wl_base.t_base_bankinfo(
phone_number String COMMENT '电话号码',
name String COMMENT '银行名称'
)
COMMENT '各银行名称及其电话号码'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/bankinfo" overwrite into table wl_base.t_base_bankinfo partition(ds=20170220)
