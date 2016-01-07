use testhive;
CREATE  TABLE  if not exists t_wrt_feed_tongji(
item_id   String ,
feed_id   String ,
user_id String  ,
content String ,
clauses String
)
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
stored as textfile ;