CREATE TABLE  if not exists wl_base.t_wrt_caiji_search_newitemid_update(
itemid String COMMENT '商品id'
)
COMMENT 'item and search id left join'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;