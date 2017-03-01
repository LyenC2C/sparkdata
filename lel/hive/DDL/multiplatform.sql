CREATE TABLE  if not exists wl_base.t_base_multiplatform(
phone String COMMENT '电话号码',
platform String COMMENT '平台',
flag String COMMENT '是否注册'
)
COMMENT '多平台注册信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/multiplatform_jiedai" overwrite into table wl_base.t_base_multiplatform partition(ds=20170227)

