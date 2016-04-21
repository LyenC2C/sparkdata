
CREATE  TABLE  if not exists t_base_ec_feed_user_buy_info (
item_id             	bigint,
user_id             	string,
cat_id              	string,
root_cat_id         	string,
root_cat_name       	string,
brand_id            	string,
brand_name          	string,
bc_type             	string,
price               	string,
tgender             	string,
tage                	int   ,
tname               	string,
tloc                	string
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;