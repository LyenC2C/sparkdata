CREATE TABLE IF NOT EXISTS t_base_ec_record_dev_new_0629 (
  item_id        BIGINT COMMENT '商品id',
  feed_id        string COMMENT '评论id',
  user_id        string COMMENT '用户id',
  content_length INT COMMENT '评论长度',
  annoy          string COMMENT '是否匿名',
  dsn            string COMMENT '评论日期',
  datediff       INT COMMENT '评论时间和当日间隔',
  sku            string COMMENT 'sku',
  title          string COMMENT '商品title',
  cat_id         string COMMENT '叶子类目id',
  root_cat_id    string COMMENT '以及类目id',
  root_cat_name  string COMMENT '一级类目名字',
  brand_id       string COMMENT '品牌id',
  brand_name     string COMMENT '品牌名字',
  bc_type        string COMMENT '店铺类型',
  price          string COMMENT '商品价格',
  shop_id        string COMMENT '店铺id',
  location       string COMMENT '店铺所在地'
)
COMMENT '淘宝电商record记录'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored AS textfile;


SET hive.exec.dynamic.partition= TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.reducers.bytes.per.reducer=500000000;


INSERT OVERWRITE TABLE t_base_ec_record_dev_new_0629 PARTITION (ds )

SELECT
  t2.*,
  t1.title,
  cat_id,
  root_cat_id,
  root_cat_name,
  brand_id,
  brand_name,
  bc_type,
  price,
  shop_id,
  location,
  CASE WHEN cat_id IS NOT NULL
    THEN 'true1'
  ELSE 'false1' END AS ds
FROM (SELECT
        cast(item_id AS BIGINT) item_id,
        title,
        cat_id,
        root_cat_id,
        root_cat_name,
        brand_id,
        brand_name,
        bc_type,
        price,
        shop_id,
        location
      FROM t_base_ec_item_dev_new
      WHERE ds = 20160621
     ) t1
 RIGHT JOIN
  (
    SELECT
      cast(item_id AS BIGINT)                                                              item_id,
      feed_id,
      user_id,
      length(content)                                                                      content_length,
      annoy,
      SUBSTRING(regexp_replace(f_date, '-', ''), 0, 8)                                  AS dsn,
      datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), SUBSTRING(f_date, 0, 10)) AS datediff,
      sku
    FROM t_base_ec_item_feed_dev
    WHERE item_id IS NOT NULL AND f_date IS NOT NULL

  ) t2 ON t1.item_id = t2.item_id
;





