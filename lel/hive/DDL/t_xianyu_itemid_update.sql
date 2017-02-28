CREATE TABLE  if not exists wlbase_dev.t_base_ec_xianyu_itemid_update(
itemid String COMMENT '商品id'
)
COMMENT '闲鱼商品id更新'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

CREATE TABLE  if not exists wl_base.t_base_ec_xianyu_itemid_update(
itemid String COMMENT '商品id'
)
COMMENT '闲鱼商品id更新'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;