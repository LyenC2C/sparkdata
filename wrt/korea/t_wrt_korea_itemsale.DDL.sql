CREATE EXTERNAL TABLE  if not exists t_wrt_korea_itemsale (
aid  string ,
amount  string ,
total  string ,
qu  string ,
st  string ,
inSale  string ,
start  string
)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA  INPATH '/user/wrt/temp/t_korea_itemsale' OVERWRITE INTO TABLE t_wrt_korea_itemsale;