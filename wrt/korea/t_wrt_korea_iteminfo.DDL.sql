CREATE EXTERNAL TABLE  if not exists t_wrt_korea_iteminfo(
    item_id String,
    title String,
    categoryId String,
    cate_name String,
    cate_root_name String,
    brandId String,
    brand_name String,
    price String,
    price_zone String,
    favor String,
    seller_id String,
    shopId String,
    country String,
    location String,
    ts String
)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


LOAD DATA  INPATH '/user/wrt/temp/t_korea_iteminfo' OVERWRITE INTO TABLE t_wrt_korea_iteminfo;