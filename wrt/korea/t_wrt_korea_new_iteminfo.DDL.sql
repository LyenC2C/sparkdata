CREATE EXTERNAL TABLE  if not exists t_wrt_korea_new_iteminfo(
    item_id String,
    title String,
    cate_name String,
    laiyuan String,
    brand_name String,
    price String,
    price_zone String,
    rateCounts String,
    item_count String,
    ts String
)
COMMENT '韩国项目2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


LOAD DATA  INPATH '/user/wrt/temp/t_korea_iteminfo' OVERWRITE INTO TABLE t_wrt_korea_new_iteminfo;