use wlbase_dev;

CREATE EXTERNAL TABLE  if not exists t_base_q_user_dev (
birthday STRING COMMENT 'yyyy-mm-dd',
phone  STRING COMMENT '�ֻ��ţ��Լ���д�����ݿ���ʧ��' ,
gender_id  BIGINT COMMENT '�Ա� 1����  2��Ů' ,
college  STRING   COMMENT 'ѧУ' ,
uin  STRING   COMMENT 'qq��' ,
lnick  STRING COMMENT '����ǩ��' ,
loc_id STRING COMMENT '���ڵر���' ,
loc STRING COMMENT '���ڵ�',
h_loc_id STRING COMMENT '�������',
h_loc STRING COMMENT '�����',
personal  STRING   COMMENT '����˵��' ,
shengxiao  STRING COMMENT '��Ф' ,
gender  BIGINT COMMENT '�Ա�  1����  2��Ů' ,
occupation  STRING  COMMENT 'ְλ' ,
constel  STRING COMMENT '����' ,
blood  STRING COMMENT 'Ѫ��' ,
url  STRING   COMMENT 'ͷ��' ,
homepage  STRING COMMENT '��ҳ' ,
nick  STRING COMMENT '�ǳ�' ,
email  STRING  COMMENT 'email' ,
uin2  STRING   COMMENT '��½�����˺�' ,
mobile  STRING COMMENT '�еĻ����ּ���139********',
ts STRING COMMENT '�ɼ�ʱ���'
)
COMMENT 'qq�û���Ϣ'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile location '/hive/external/wlbase_dev/t_base_q_user_dev/';


-- select birthday,uin  from t_base_q_user_dev where constel='-' limit 100;

select locid, count(1) from (select case when LENGTH(loc)>3 then 1  else 0 end  as locid from t_base_q_user_dev) t GROUP  locid;