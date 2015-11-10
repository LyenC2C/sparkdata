CREATE EXTERNAL TABLE  if not exists t_base_ec_shop_dev (
shop_id STRING  COMMENT '����id' ,
seller_id STRING  COMMENT '����id' ,
shop_name STRING  COMMENT '��������' ,
seller_name STRING  COMMENT '��������' ,
star STRING  COMMENT '�Ǽ�����èΪ99' ,
credit STRING  COMMENT '�����ȼ�' ,
starts STRING COMMENT '����ʱ��',
bc_type STRING COMMENT '��������',
item_count BIGINT COMMENT '��Ʒ����',
fans_count BIGINT   COMMENT '��˿��' ,
good_rate_p FLOAT   COMMENT '������' ,
weitao_id STRING  COMMENT '΢��id' ,
desc_score FLOAT  COMMENT '������' ,
service_score FLOAT  COMMENT '�����' ,
wuliu_score FLOAT  COMMENT '������' ,
location  String COMMENT '��ַ' ,
ts STRING COMMENT '�ɼ�ʱ���'
)
COMMENT '���̵��̻�����Ϣ��'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n';
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_shop_dev' ;
