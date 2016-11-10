CREATE EXTERNAL TABLE  if not exists t_base_city_province_dev (
city String COMMENT '城市',
province String COMMENT '省份'
)
COMMENT '全国城市对应省份列表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' ;

--LOAD DATA  INPATH '/user/wrt/city_pro' OVERWRITE INTO TABLE t_base_city_province_dev