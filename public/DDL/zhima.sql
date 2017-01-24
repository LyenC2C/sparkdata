CREATE  TABLE  if not exists t_base_credit_zhima_score (
userid        string   comment '用户淘宝id',
tel_index     string   comment  '手机号索引',
zhima_score   int        comment  '芝麻分',
)
COMMENT '支付宝芝麻分'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   LINES TERMINATED BY '\n'
stored as textfile ;