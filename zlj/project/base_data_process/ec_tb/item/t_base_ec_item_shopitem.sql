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
COMMENT '�Ա�������Ʒ����  �ܹ�7.6��'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
stored as textfile ;
