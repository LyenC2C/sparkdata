create table wl_service.t_lel_record_backflow_multi_fields_standard(phone string,idbank string,idcard string,name string,latest string,times int,cate string)
COMMENT 'record回流数据统计队列'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;