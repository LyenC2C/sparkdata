use testhive;
CREATE  TABLE  if not exists t_wrt_feed_tongji3(
item_id   String ,
feed_id   String ,
user_id String  ,
content String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;