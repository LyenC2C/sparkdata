CREATE EXTERNAL TABLE  if not exists t_wrt_znk_brandid_name (
brand_id String COMMENT '品牌id',
brand_name String COMMENT '品牌名称'
)
COMMENT '纸尿裤品牌id对应品牌名称'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;

LOAD DATA  INPATH '/user/wrt/temp/znk_brandid_name' OVERWRITE INTO TABLE t_wrt_znk_brandid_name;

CREATE TABLE t_wrt_znk_brand_brandname (
brand String COMMENT '品牌模糊名称',
brand_name String COMMENT '品牌正规名称'
)
COMMENT '纸尿裤模糊品牌到正规品牌'

LOAD DATA  INPATH '/user/wrt/temp/znk_brand_brandname' OVERWRITE INTO TABLE t_wrt_znk_brand_brandname;