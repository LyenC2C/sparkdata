CREATE TABLE  if not exists wl_base.t_base_phone_sougou_400_800(
word String COMMENT '号码',
source String COMMENT '搜索来源',
tag String COMMENT '标签',
amount int COMMENT '总计',
tel_co String COMMENT '未知',
place String COMMENT '地点',
platform String COMMENT '搜索平台'
)
COMMENT '搜狗400和800电话搜索'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/sougou_400_800" overwrite into table wl_base.t_base_phone_sougou_400_800 partition(ds=20170216)

