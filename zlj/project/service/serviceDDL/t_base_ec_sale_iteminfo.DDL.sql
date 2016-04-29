
CREATE  TABLE  if not exists t_base_ec_sale_iteminfo (
item_id             	string,
cat_id              	string,
cat_name            	string,
root_cat_id         	string,
root_cat_name       	string,
brand_id            	string,
brand_name          	string,
bc_type             	string,
price               	string,
location            	string,
shop_id             	string,
r_price             	float ,
s_price             	float ,
total_sold          	bigint
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;