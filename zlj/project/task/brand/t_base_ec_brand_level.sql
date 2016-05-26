


CREATE  TABLE  if not exists t_base_ec_brand_level (
cat_id   string COMMENT    '类目id ',
cat_name  string  COMMENT '类目名字 ',
cat_level  string  COMMENT '类目等级',
brand_id  string  COMMENT  '品牌名称',
brand_name  string  COMMENT '品牌名称' ,
item_num  bigint  ,
brand_level bigint  COMMENT '品牌等级',
rank_num  bigint
)
COMMENT '品牌分级'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;
