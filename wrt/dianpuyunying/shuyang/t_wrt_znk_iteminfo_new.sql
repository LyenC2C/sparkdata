CREATE EXTERNAL TABLE  if not exists t_wrt_znk_iteminfo_new (
item_id String COMMENT '商品id',
title String COMMENT '商品名称',
brand_name String COMMENT '商品品牌',
item_size String COMMENT '纸尿裤尺寸',
item_type String COMMENT '纸尿裤类型',
item_count String COMMENT '纸尿裤片数',
price String COMMENT '纸尿裤价格',
pic_url String COMMENT '图片url'
)
COMMENT '纸尿裤项目商品重要信息表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n';

LOAD DATA  INPATH '/user/wrt/temp/znk_item_complete_info' OVERWRITE INTO TABLE t_wrt_znk_iteminfo_new PARTITION (ds='20160901');
LOAD DATA  INPATH '/user/wrt/temp/znk_repair_iteminfo' OVERWRITE INTO TABLE t_wrt_znk_iteminfo_new PARTITION (ds='20160912');