

use wlbase_dev;
drop table   t_zlj_zj_shop;

CREATE  TABLE  if not exists t_zlj_zj_shop (
shop_id string ,
shop_title string ,
location string
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
stored as textfile ;

