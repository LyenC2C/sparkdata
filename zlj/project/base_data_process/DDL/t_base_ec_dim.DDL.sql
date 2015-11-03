CREATE  TABLE  if not exists t_base_ec_dim (
cate_id         					 bigint     COMMENT '��Ŀid                                    ',
cate_name       					 string     COMMENT '��Ŀ����                                ',
cate_level      					 bigint     COMMENT '��Ŀ�㼶                                ',
cate_level1_id  					 bigint     COMMENT 'һ����Ŀid                              ',
cate_level2_id  					 bigint     COMMENT '������Ŀid                              ',
cate_level3_id  					 bigint     COMMENT '������Ŀid                              ',
cate_level4_id  					 bigint     COMMENT '�ļ���Ŀid                              ',
cate_level5_id  					 bigint     COMMENT '�弶��Ŀid                              ',
cate_level1_name					 string     COMMENT 'һ����Ŀ����                          ',
cate_level2_name					 string     COMMENT '������Ŀ����                          ',
cate_level3_name					 string     COMMENT '������Ŀ����                          ',
cate_level4_name					 string     COMMENT '�ļ���Ŀ����                          ',
cate_level5_name					 string     COMMENT '�弶��Ŀ����                          ',
cate_full_name  					 string     COMMENT '��Ŀȫ·��                             ',
is_leaf         					 string     COMMENT '�Ƿ�Ҷ����Ŀ                          ',
parent_id       					 bigint     COMMENT '����ĿID                                 ',
parent_name     					 string     COMMENT '����Ŀ����                             ',
features        					 string     COMMENT '��Ŀ����                                ',
is_virtual      					 string     COMMENT '�Ƿ�������Ŀ                          ',
properties      					 string     COMMENT '��Ŀ����                                ',
commodity_id    					 bigint     COMMENT 'Ʒ��ID                                    ',
commodity_name  					 string     COMMENT 'Ʒ������                                ',
deleted         					 string     COMMENT '�Ƿ�ɾ����0:δɾ����1��ɾ����2������ ',
cate_type       					 bigint     COMMENT '�г���Ϣ����λȡ��0=ȱʡ��1=�Ա���2=��è ',
industry_id     					 string     COMMENT '�ϵ���ҵid                              ',
industry_name   					 string     COMMENT '�ϵ���ҵ����                          ',
is_industry_valid 					 string     COMMENT ' ��ҵ�Ƿ���Ч Y:��Ч��N��ʧЧ    ',
industry_360_id 				 	string     COMMENT '360��ҵid                                 ',
industry_360_name 					string     COMMENT ' 360��ҵ����                             ',
is_industry_360_valid				string     COMMENT ' 360��ҵ�Ƿ���Ч Y:��Ч��N��ʧЧ ',
industry_his_id 					string     COMMENT ' ��ʷ��ҵid                              ',
industry_his_name 					string     COMMENT ' ��ʷ��ҵ����                          ',
is_industry_his_valid				string     COMMENT ' ��ʷ��ҵ�Ƿ���Ч Y:��Ч��N��ʧЧ ',
stat_date       					string      COMMENT ' ��ʽ��YYYYMMDD                           '
)
COMMENT '������Ŀ����'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;