CREATE EXTERNAL TABLE  if not exists t_base_ec_item_feed_dev_zlj (
item_id STRING  COMMENT  '��Ʒid',
item_title STRING  COMMENT  '��Ʒtitle',
feed_id STRING COMMENT '����id',
user_id STRING COMMENT '�û�id',
content STRING COMMENT '��������',
f_date STRING COMMENT '��������',
annoy STRING COMMENT '�Ƿ�����',
ts STRING COMMENT '�ɼ�ʱ���'
)
COMMENT '������Ʒ�û����۱�'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_item_feed_dev/';


CREATE EXTERNAL TABLE  if not exists t_base_ec_item_feed_dev (
item_id STRING  COMMENT  '��Ʒid',
item_title STRING  COMMENT  '��Ʒtitle',
feed_id STRING COMMENT '����id',
user_id STRING COMMENT '�û�id',
content STRING COMMENT '��������',
f_date STRING COMMENT '��������',
annoy STRING COMMENT '�Ƿ�����',
ts STRING COMMENT '�ɼ�ʱ���'
)
COMMENT '������Ʒ�û����۱�'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n' ;