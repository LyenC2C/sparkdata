

use wlbase_dev;

CREATE  TABLE  if not exists t_zlj_base_ec_item_sale_dev_day (
shop_id   string  ,
item_id   string ,
price    FLOAT ,
sold_num  FLOAT ,
day_sold_price      	FLOAT
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

