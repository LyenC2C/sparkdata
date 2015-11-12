use wlbase_dev;
CREATE  TABLE  if not exists t_wrt_item_tag_level(
shop_id STRING,
shop_local STRING
)
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
stored as textfile ;