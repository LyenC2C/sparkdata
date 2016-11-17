-- CREATE EXTERNAL TABLE  if not exists wlbase_dev.t_base_ec_record_dev (
-- item_id STRING ,
-- feed_id STRING ,
-- user_id STRING ,
-- content_length BIGINT ,
-- annoy BIGINT ,
-- ds STRING ,
-- datediff BIGINT,
-- title STRING,
-- cat_id STRING ,
-- root_cat_id STRING,
-- root_cat_name  STRING ,
-- brand_id STRING ,
-- brand_name  STRING,
-- bc_type STRING,
-- price double  ,
-- location STRING
-- )
-- COMMENT '评价 商品详情join表'
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
-- stored as textfile location '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev/';


CREATE  TABLE  if not exists wlbase_dev.t_base_ec_record_dev_new_t (
item_id                 bigint    COMMENT '商品id' ,
feed_id                 string    COMMENT '评论id' ,
user_id                 string    COMMENT '用户id' ,
content_length          int       COMMENT '评论长度' ,
annoy                   string    COMMENT '是否匿名' ,
dsn                     string    COMMENT '评论日期' ,
datediff                int       COMMENT '时间差' ,
date_predict            string    COMMENT '根据评论分析的购买时间' ,
sku                     string    COMMENT 'sku' ,
title                   string    COMMENT '商品title' ,
cat_id                  string    COMMENT '叶子类目id' ,
root_cat_id             string    COMMENT '一级类目id' ,
root_cat_name           string    COMMENT '一类类目名称' ,
brand_id                string    COMMENT '品牌id' ,
brand_name              string    COMMENT '品牌名字' ,
bc_type                 string    COMMENT 'b天猫c集市' ,
price                   string    COMMENT '价格' ,
shop_id                 string    COMMENT '店铺id' ,
location                string    COMMENT '店铺地址' ,
tel_index               string    COMMENT '手机号编码' ,
tel_user_rn             int       COMMENT '手机号对应淘宝id次数编号'
)
COMMENT '用户购买记录评价 商品详情join表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile location '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev/';


SET hive.exec.dynamic.partition= TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.reducers.bytes.per.reducer=500000000;

Drop table t_zlj_tmp ;
create table t_zlj_tmp  like t_base_ec_record_dev_new_t ;
INSERT  overwrite table t_zlj_tmp PARTITION(ds)
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
  t3.tel_index ,
  t3.rn as tel_user_rn,
  CASE WHEN cat_id IS NOT NULL
    THEN 'true1'
  ELSE 'false1' END AS ds
FROM (
       SELECT
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
      WHERE ds = 20161013
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
      case when content='评价方未及时做出评价,系统默认好评\!' then date_add(f_date,-22)
           when content='好评\！' then date_add(f_date,-11)
           else  date_add(f_date,-9)
      end  as date_predict ,
      sku
   FROM t_base_ec_item_feed_dev_inc_new
        WHERE ds='20160926' and item_id IS NOT NULL AND f_date IS NOT NULL
  ) t2 ON t1.item_id = t2.item_id
  left join t_zlj_phone_rank_index t3 on t2.user_id=t3.tb_id
;
