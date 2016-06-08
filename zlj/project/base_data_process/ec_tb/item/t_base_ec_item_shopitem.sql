CREATE  TABLE  if not exists t_base_ec_item_shopitem (
title    String ,
sold    String ,
reservePrice    String ,
maxpage    String ,
rate    String ,
picUrl    String ,
item_id    String ,
page    String ,
salePrice    String
)
COMMENT '淘宝店铺商品数据  总共7.6亿'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
stored as textfile ;
