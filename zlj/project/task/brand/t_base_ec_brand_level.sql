


CREATE  TABLE  if not exists t_base_ec_brand_level (
cat_id   string COMMENT    '��Ŀid ',
cat_name  string  COMMENT '��Ŀ���� ',
cat_level  string  COMMENT '��Ŀ�ȼ�',
brand_id  string  COMMENT  'Ʒ������',
brand_name  string  COMMENT 'Ʒ������' ,
item_num  bigint  ,
brand_level bigint  COMMENT 'Ʒ�Ƶȼ�',
rank_num  bigint
)
COMMENT 'Ʒ�Ʒּ�'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;
