create table wl_service.t_lel_record_backflow_phone_idcard(mark string,latest_1m int,latest_3m int,latest_6m int,latest_12m int,cate string)
COMMENT 'record回流数据被查询记录'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;