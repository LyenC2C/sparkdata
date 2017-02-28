CREATE TABLE  if not exists wl_base.t_base_creditbank_2(
phone String COMMENT '电话号码',
bank String COMMENT '银行名称',
source String COMMENT '信息来源'
)
COMMENT '银行信用卡相关'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;



insert into table wl_base.t_base_phone_name_credit_bank partition(ds=20170224)
