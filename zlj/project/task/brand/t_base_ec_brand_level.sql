


CREATE  TABLE  if not exists t_base_ec_brand_level (
cat_id   string COMMENT    '???id ',
cat_name  string  COMMENT '??????? ',
cat_level  string  COMMENT '??????',
brand_id  string  COMMENT  '???????',
brand_name  string  COMMENT '???????' ,
item_num  bigint  ,
brand_level bigint  COMMENT '?????',
rank_num  bigint
)
COMMENT '?????'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;
