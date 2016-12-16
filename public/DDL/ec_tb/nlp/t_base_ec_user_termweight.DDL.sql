
CREATE EXTERNAL TABLE  if not exists wlbase_dev.t_base_ec_user_termweight (
user_id STRING  COMMENT  '用户id',
term_weight  STRING   COMMENT '商品title权重 用|隔开每个词'
)
COMMENT '电商用户 商品title权重分析'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   ;


  LOAD DATA     INPATH '/user/zlj/temp/termweight1228/' OVERWRITE
  INTO TABLE wlbase_dev.t_base_ec_user_termweight PARTITION (ds='20161216') ;


