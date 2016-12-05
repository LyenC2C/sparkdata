


ds=$1

/home/yarn/hive/bin/hive<<EOF

use wlbase_dev;

SET hive.exec.dynamic.partition= TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.reducers.bytes.per.reducer=500000000;
Drop  table  if EXISTS  t_base_ec_record_dev_new_tmp  ;


LOAD DATA  INPATH "/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.$ds/"  INTO TABLE t_base_ec_item_feed_dev_inc_new PARTITION (ds='$1');



Drop table t_base_ec_record_dev_new_tmp ;
create table t_base_ec_record_dev_new_tmp like t_base_ec_record_dev_new;

INSERT OVERWRITE TABLE t_base_ec_record_dev_new_tmp PARTITION (ds)


SELECT
  t2.*,
  t1.title,
  cat_id,
  root_cat_id,
  root_cat_name,
  brand_id,
  brand_name,
  bc_type,
  price,
  shop_id,
  location,
  CASE WHEN cat_id IS NOT NULL
    THEN 'true1'
  ELSE 'false1' END AS ds
FROM (SELECT
        cast(item_id AS BIGINT) item_id,
        title,
        cat_id,
        root_cat_id,
        root_cat_name,
        brand_id,
        brand_name,
        bc_type,
        price,
        shop_id,
        location
      FROM t_base_ec_item_dev_new
      WHERE ds = 20161202
     ) t1
 RIGHT JOIN
  (
    SELECT
      cast(item_id AS BIGINT)                                                              item_id,
      feed_id,
      user_id,
      length(content)                                                                      content_length,
      annoy,
      SUBSTRING(regexp_replace(f_date, '-', ''), 0, 8)                                  AS dsn,
      datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), SUBSTRING(f_date, 0, 10)) AS datediff,
      sku

   FROM t_base_ec_item_feed_dev_inc_new
        WHERE ds='$ds' and item_id IS NOT NULL AND f_date IS NOT NULL

  ) t2 ON t1.item_id = t2.item_id
;


EOF


tmp_true_path='/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_tmp/ds=true1'

tmp_false_path='/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_tmp/ds=false1'


hadoop fs -cat $tmp_true_path/* >/mnt/raid2/zlj/cmt_inc_data_$ds

hadoop fs -rm   /hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=true/cmt_inc_data_$ds

hadoop fs -put  /mnt/raid2/zlj/cmt_inc_data_$ds  /hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=true/

rm  /mnt/raid2/zlj/cmt_inc_data_$ds


hadoop fs -cat $tmp_false_path/* >/mnt/raid2/zlj/cmt_inc_data_$ds

hadoop fs -rm   /hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=false/cmt_inc_data_$ds

hadoop fs -put  /mnt/raid2/zlj/cmt_inc_data_$ds  /hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=false/

rm  /mnt/raid2/zlj/cmt_inc_data_$ds