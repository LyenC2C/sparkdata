CREATE  TABLE  if not exists t_zlj_ec_maxfeedId (
brand_id   String COMMENT '品牌id',
brand_name  String
)
COMMENT '电商品牌数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;