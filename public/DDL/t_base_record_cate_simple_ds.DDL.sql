CREATE  TABLE   if not exists wl_analysis.t_base_record_cate_simple_ds (
item_id                 bigint      COMMENT '商品id',
feed_id                 string      COMMENT '评论id',
user_id                 string      COMMENT '用户id',
content_length          int         COMMENT '评论长度',
annoy                   string      COMMENT '是否匿名',
dsn                     string      COMMENT '评论日期',
datediff                int         COMMENT '',
cat_id                  string      COMMENT '叶子类目id',
root_cat_id             string      COMMENT '一级类目id',
root_cat_name           string      COMMENT '一级类目名字',
cate_level1_id          bigint      COMMENT '一级类目id',
cate_level2_id          bigint      COMMENT '二级类目id',
cate_level3_id          bigint      COMMENT '三级类目id',
cate_level4_id          bigint      COMMENT '四级类目id',
cate_level5_id          bigint      COMMENT '五级类目id',
brand_id                string      COMMENT '品牌id',
brand_name              string      COMMENT '品牌名',
bc_type                 string      COMMENT 'bc类型',
price                   float       COMMENT '价格',
shop_id                 string      COMMENT '店铺id',
location                string      COMMENT '店铺地址',
tel_index               string      COMMENT '电话索引',
tel_user_rn             int         COMMENT '电话对应用户排序编号'

 )
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1500;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;


-- 初次更新
INSERT INTO TABLE wl_analysis.t_base_record_cate_simple_ds PARTITION (ds)
SELECT
  item_id            ,
feed_id            ,
user_id            ,
content_length     ,
annoy              ,
dsn                ,
datediff      		 ,
cat_id               ,
root_cat_id          ,
root_cat_name        ,
cate_level1_id       ,
cate_level2_id       ,
cate_level3_id       ,
cate_level4_id       ,
cate_level5_id       ,
brand_id           ,
brand_name        ,
bc_type            ,
price           	 ,
shop_id            ,
location           ,
tel_index          ,
tel_user_rn         ,
  SUBSTRING (regexp_replace(f_date,'-',''),0,8) ds
FROM
 wlrefer.t_zlj_phone_rank_index t3
 join (

SELECT
item_id        ,
feed_id        ,
user_id        ,
content_length ,
annoy          ,
dsn            ,
datediff       ,
sku            ,
title          ,
cat_id         ,
root_cat_id    ,
root_cat_name  ,
brand_id       ,
brand_name     ,
bc_type        ,
price          ,
shop_id        ,
location       ,
  cate_level1_id,
  cate_level2_id,
  cate_level3_id,
  cate_level4_id,
  cate_level5_id
  from
       wlbase_dev.t_base_ec_dim t1
   join
     wl_base.t_base_ec_record_dev_new t2 on t2.ds='true' and t1.ds = 20161122 and t1.cate_id =t2.cat_id
  ) t2
--   left
     on t2.user_id=t3.tb_id
 where tel_index is not null and tel_user_rn<4 and price<160000
and  root_cat_id is not null
;


SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1500;
SET hive.exec.max.dynamic.partitions=2000;
set hive.exec.reducers.bytes.per.reducer=500000000;

INSERT INTO TABLE wl_analysis.t_base_record_cate_simple_ds PARTITION (ds)
SELECT
  item_id            ,
feed_id            ,
user_id            ,
content_length     ,
annoy              ,
dsn                ,
datediff      		 ,
cat_id               ,
root_cat_id          ,
root_cat_name        ,
cate_level1_id       ,
cate_level2_id       ,
cate_level3_id       ,
cate_level4_id       ,
cate_level5_id       ,
brand_id           ,
brand_name        ,
bc_type            ,
price           	 ,
shop_id            ,
location           ,
tel_index          ,
rn as tel_user_rn         ,
  SUBSTRING (dsn,0,6) ds
FROM
 wlrefer.t_zlj_phone_rank_index t3
 join (
SELECT
item_id        ,
feed_id        ,
user_id        ,
content_length ,
annoy          ,
dsn            ,
datediff       ,
sku            ,
title          ,
cat_id         ,
root_cat_id    ,
root_cat_name  ,
brand_id       ,
brand_name     ,
bc_type        ,
price          ,
shop_id        ,
location       ,
  cate_level1_id,
  cate_level2_id,
  cate_level3_id,
  cate_level4_id,
  cate_level5_id
  from
       wlbase_dev.t_base_ec_dim t1
   join
     wl_base.t_base_ec_record_dev_new_inc t2 on t2.ds=20170220  and t1.ds = 20161122 and t1.cate_id =t2.cat_id
     where  price<160000  and  root_cat_id is not null
  ) t2
--   left
     on t2.user_id=t3.tb_id
 where tel_index is not null and rn<4
;

-- t_base_ec_record_dev_new_inc update
-- ds=20170107
-- ds=20170108
-- ds=20170114
-- ds=20170118
-- ds=20170121
-- ds=20170203
-- ds=20170212
-- ds=20170220
