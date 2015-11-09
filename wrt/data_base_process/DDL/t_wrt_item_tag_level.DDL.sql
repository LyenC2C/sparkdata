use wlbase_dev;
CREATE  TABLE  if not exists t_wrt_item_tag_level(
brand_id   String COMMENT '品牌id',
brand_name  String ,
stars String ,
brand_level String ,
brand_tag String
)
COMMENT '电商品牌标签等级表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
stored as textfile ;