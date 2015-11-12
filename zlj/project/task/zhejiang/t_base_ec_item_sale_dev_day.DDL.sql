

CREATE  TABLE  if not exists t_zlj_base_ec_item_sale_dev_day (
shop_id   string  ,
item_id   string ,
s_num      	FLOAT ,
price    FLOAT ,
s_price  FLOAT

)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

