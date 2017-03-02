CREATE TABLE  if not exists wl_base.t_base_phone_huangye88_daikuan(
phone String COMMENT '电话号码',
name String COMMENT '贷款名称',
area String COMMENT '地点',
company String COMMENT '公司名称',
contact String COMMENT '联系人称呼',
pinpai String COMMENT '同贷款名称',
cate String COMMENT '类型',
tiezi_id String COMMENT '暂且未知',
detail String COMMENT '详细介绍',
update String COMMENT '更新时间'
)
COMMENT '黄也88搜索有关贷款类信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/huangye88_daikuan" overwrite into table wl_base.t_base_phone_huanye88_daikuan partition(ds=20170216)

