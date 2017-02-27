CREATE TABLE  if not exists wl_base.t_base_guangfa_credit_customer(
phone String COMMENT '电话号码'
)
COMMENT '广发银行信用卡客户电话号码'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath '/user/lel/results/guangfa' into table wl_base.t_base_guangfa_credit_customer partition(ds=20170222)
