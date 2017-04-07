CREATE TABLE  if not exists wl_base.t_base_phone_dianhuabang(
phone String COMMENT '区号+电话号码',
acode String COMMENT '区号',
phone_num String COMMENT '电话',
name String COMMENT '名称',
addr String COMMENT '地址',
cate String COMMENT '类别',
cid String COMMENT '城市id',
city_name String COMMENT '城市名字'
)
COMMENT '电话邦'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;


load data inpath "/user/lel/temp/dianhuabang" overwrite into table wl_base.t_base_phone_dianhuabang partition(ds=20170407)

