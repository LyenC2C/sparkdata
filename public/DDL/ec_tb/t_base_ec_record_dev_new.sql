
CREATE EXTERNAL TABLE  if not exists wlbase_dev.t_base_ec_record_dev_new_t (
item_id                 bigint       COMMENT '商品id' ,
feed_id                 string       COMMENT '评论id' ,
user_id                 string       COMMENT '用户id' ,
content_length          int          COMMENT '评论长度' ,
annoy                   string       COMMENT '是否匿名' ,
dsn                     string       COMMENT '评论日期' ,
datediff                int          COMMENT '评论日期到当前时间差' ,
date_predict            string       comment '根据评论预测的商品购买时间' ,
sku                     string       COMMENT '' ,
title                   string       COMMENT '商品title' ,
cat_id                  string       COMMENT '叶子类目id' ,
root_cat_id             string       COMMENT '一级类目id' ,
root_cat_name           string       COMMENT '一级类目名' ,
brand_id                string       COMMENT '品牌id' ,
brand_name              string       COMMENT '品牌名' ,
bc_type                 string       COMMENT 'BC 类型' ,
price                   float        COMMENT '价格' ,
shop_id                 string       COMMENT '店铺id' ,
location                string       COMMENT '店铺地址' ,
tel_index               string       comment '手机号索引' ,
tel_user_rn             int          COmment '手机号对应多个用户的排序id' ,
ds                      string       COMMENT '分区 true、false  false为商品在商品库不存在'
)
COMMENT '商品评论关联表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile location '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_t/';
