CREATE EXTERNAL TABLE  if not exists t_base_ec_item_sale_dev (
item_id	STRING  COMMENT  '��Ʒid',

r_price float COMMENT 'ԭ��',
s_price float COMMENT '�ۼ�',
bc_type STRING COMMENT '�Ա�C ��èB',
quantity BIGINT	COMMENT '���',
total_sold BIGINT COMMENT '������',
order_cost BIGINT COMMENT '',
shop_id	STRING COMMENT '����id',
ts	STRING COMMENT '�ɼ�ʱ���'
)
COMMENT '������Ʒ�û�����״̬��'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' ;
-- stored as textfile location '/hive/external/wlbase_dev/t_base_ec_item_sale_dev/';
-- ����title
