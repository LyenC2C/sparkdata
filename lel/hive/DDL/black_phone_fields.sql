CREATE TABLE  if not exists wl_base.t_base_black_phone_fields(
phone_field String COMMENT '电话号码字段'
)
COMMENT '黑产电话号码字段'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/black_phone_fields" overwrite into table wl_base.t_base_black_phone_fields partition(ds=20170220)
