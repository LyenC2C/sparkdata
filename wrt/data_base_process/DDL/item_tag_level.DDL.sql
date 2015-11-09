CREATE  TABLE  if not exists item_tag_level(
brand_id   String COMMENT '品牌id',
brand_name  String ,
brand_level String ,
brand_tag String ,
)
COMMENT '电商品牌标签等级表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;