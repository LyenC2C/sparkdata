CREATE TABLE  if not exists wl_base.t_base_phone_dianhuabang(
phone String COMMENT '电话号码',
name String COMMENT '场所名称',
addr String COMMENT '地址',
time String COMMENT '时间戳'
)
COMMENT '电话邦'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/dianhuabang" overwrite into table wl_base.t_base_phone_dianhuabang partition(ds=20170216)

