CREATE  TABLE  if not exists t_base_ec_item_title_cut (
item_id   String COMMENT '',
title_cut   String COMMENT '',
title_cut_Stag   String COMMENT ''
)
COMMENT ''
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;