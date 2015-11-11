use wlbase_dev;
CREATE  TABLE  if not exists t_wrt_test_tmallint_id(
item_id	 STRING  COMMENT  '部分国际商品id'
)
COMMENT '部分国际商品销量测试'
PARTITIONED BY  (ds STRING )
stored as textfile ;