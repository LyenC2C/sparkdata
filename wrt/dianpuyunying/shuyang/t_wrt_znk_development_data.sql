
CREATE EXTERNAL TABLE  if not exists t_wrt_znk_development_data (
feed_id String COMMENT '商品id',
user_id String COMMENT '商品id',
item_id String COMMENT '商品id',
dsn String COMMENT '评论时间',
title String COMMENT '商品名称',
brand_name String COMMENT '商品品牌',
item_size String COMMENT '纸尿裤尺寸',
item_type String COMMENT '纸尿裤类型',
item_count String COMMENT '纸尿裤片数',
price String COMMENT '纸尿裤价格',
pic_url String COMMENT '图片url',
sold String COMMENT '商品销量'
)
COMMENT '纸尿裤项目开发所需数据产出'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n';

insert into table t_wrt_znk_development_data partition(ds = 20160919)
SELECT
tt2.feed_id,
tt2.user_id,
tt2.item_id,
tt2.dsn,
tt1.title,
tt1.brand_name,
tt1.item_size,
tt1.item_type,
tt1.item_count,
tt1.price,
tt1.pic_url,
tt1.sold
FROM
(
select t1.*,
case when t2.item_id is null then "-" else t2.total end as sold
FROM
(select * from t_wrt_znk_iteminfo_new where ds = 20160901)t1
left JOIN
(select item_id,total from wlbase_dev.t_base_ec_item_sold_dev where ds = 20160816)t2
ON
t1.item_id = t2.item_id
)tt1
JOIN
(
select * from t_wrt_znk_record where ds = 20160829
)tt2
ON
tt1.item_id = tt2.item_id;


