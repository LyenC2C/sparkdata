CREATE TABLE  if not exists wl_base.t_base_touzi_licai(
phone String COMMENT '电话号码',
platform String COMMENT '平台',
flag String COMMENT '是否注册'
)
COMMENT '投资理财'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/yingxiao/touzi_licai/ningbo" overwrite into table wl_base.t_base_touzi_licai partition(ds=20170329)

