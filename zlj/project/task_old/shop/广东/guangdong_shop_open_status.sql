CREATE  TABLE  if not exists guangdong_shop_open_status (
shop_id  bigint    COMMENT '一级类目id ',
opened string COMMENT '是否关闭'
)
COMMENT '广东店铺状态'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;