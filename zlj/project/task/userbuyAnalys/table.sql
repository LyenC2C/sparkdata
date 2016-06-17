CREATE  TABLE  if not exists t_base_ec_record_dev_new_617 (
item_id                 bigint,
feed_id                 string,
user_id                 string,
content_length          int   ,
annoy                   string,
ds                      string,
datediff                int   ,
sku                     string,
title                   string,
cat_id                  string,
root_cat_id             string,
root_cat_name           string,
brand_id                string,
brand_name              string,
bc_type                 string,
price                   string,
shop_id                 string,
location                string
)
COMMENT '商品记录表'
PARTITIONED BY  (dsn STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;