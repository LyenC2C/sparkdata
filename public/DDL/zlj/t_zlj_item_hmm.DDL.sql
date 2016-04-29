

CREATE  TABLE  if not exists t_zlj_item_hmm (
item_id   String COMMENT 'id',
item_name  String ,
root_cat_id String,
root_cat_name String,
hmm String
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



CREATE  TABLE  if not exists t_zlj_temp (
id1   String COMMENT 'id',
id2   String COMMENT 'id'
)
PARTITIONED BY  (ds STRING )