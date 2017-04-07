create table wl_base.t_base_unusual_iteminfo (
item_id string comment '商品id',
rec_str string comment '推荐该商品的商品信息(商品间由\002分隔,每个商品包括id,卖家id,类目id,推荐方式)',
seller_id string comment '卖家id',
title string comment '商品名称',
picurl string comment '图片url',
r_price string comment '原价',
price string comment '现价',
sold string comment '总销量',
score string comment '分数(不知道啥分数)',
categoryId string comment '类目id',
cat_name string comment '类目名称',
root_cat_id string comment '根类目id',
root_cat_name string comment '根类目名称',
ts string comment '时间戳'
)
COMMENT '异常商品与推荐来源信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;

load data inpath