    CREATE TABLE  if not exists wl_base.t_base_credit_bank(
    phone String COMMENT '电话号码',
    platform String COMMENT '平台',
    flag String COMMENT '是否注册'
    )
    COMMENT '银行信用卡注册信息'
    PARTITIONED BY  (ds STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/credit_bank" overwrite into table wl_base.t_base_credit_bank partition(ds=20170221)

