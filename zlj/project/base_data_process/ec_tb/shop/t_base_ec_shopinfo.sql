CREATE  TABLE  if not exists t_base_ec_shopinfo (
shop_age  string,
shop_id  string,
user_id  string,
company_name  string,
shop_link  string,
licence  string,
seller  string,
bail  string,
shop_name  string,
shop_type  map<string, string>   COMMENT '店铺类型参数表'
)
COMMENT '淘宝店铺数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'    COLLECTION ITEMS TERMINATED BY ','   MAP KEYS TERMINATED BY ':'
stored as textfile ;
