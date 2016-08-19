CREATE EXTERNAL TABLE  if not exists t_base_ec_shopitem_new (
shop_id String COMMENT '店铺id',
item_id String COMMENT '商品id',
item_title String COMMENT '商品title',
picUrl String COMMENT '商品图片url',
sold BIGINT COMMENT '销量',
reservePrice float COMMENT '商品原价',
salePrice float COMMENT '商品现价',
bctype String COMMENT '商品类型（淘宝天猫）',
up_day String COMMENT '上架日期',
down_day String COMMENT '下架日期（未下架用0）',
ts String COMMENT '采集时间戳'
)
COMMENT '新店铺的商品列表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;

-- 新的数据源，网页上爬取的
-- 此表用于：
-- 1.给采集组提供最新的item_id,其中item_id可以按照销量排序
use wlservice;
create table t_wrt_upgrade_itemid AS
select item_id,sold from wlbase_dev.t_base_ec_shopitem_new where ds = 20160722 order by sold desc
