CREATE TABLE  if not exists wl_base.t_base_phone_sougou_bbs(
phone String COMMENT '电话号码',
content String COMMENT '内容',
url String COMMENT 'url',
cacheresult_info String COMMENT '缓存结果信息',
blog_info String COMMENT '博客信息',
date String COMMENT '时间',
title String COMMENT '标题',
time_stamp String COMMENT '时间戳'
)
COMMENT '搜狗论坛搜索'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/sougou_bbs_search" overwrite into table wl_base.t_base_phone_sougou_bbs partition(ds=20170216)

