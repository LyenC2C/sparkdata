CREATE  TABLE  if not exists t_zlj_ec_maxfeedId (
brand_id   String COMMENT 'Ʒ��id',
brand_name  String
)
COMMENT '����Ʒ������'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;