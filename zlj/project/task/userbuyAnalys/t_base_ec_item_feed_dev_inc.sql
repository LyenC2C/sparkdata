



ds=$1

/home/zlj/hive/bin/hive<<EOF



USE wlbase_dev;

LOAD DATA  INPATH "/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.$ds/"  INTO TABLE t_base_ec_item_feed_dev_inc PARTITION (ds='$1');


DROP  TABLE  IF EXISTS   t_base_ec_item_feed_dev_inc_tmp;

CREATE TABLE t_base_ec_item_feed_dev_inc_tmp
  AS
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
      location
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
            location
          FROM t_base_ec_item_dev
          WHERE ds = 20160216
          ) t1
      JOIN
      (
        SELECT
          cast(item_id AS BIGINT)      item_id,
          feed_id,
          user_id,
          length(content)              content_length,
          annoy,
          SUBSTRING (regexp_replace(f_date,'-',''),0,8)  as ds ,
          datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), SUBSTRING (f_date,0,10)) AS datediff
        FROM t_base_ec_item_feed_dev_inc
        WHERE ds='$ds' and item_id IS NOT NULL AND f_date IS NOT NULL AND regexp_replace(f_date, '-', '') > 20150101

      ) t2 ON t1.item_id = t2.item_id;


EOF

hadoop fs -cat /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_inc_tmp/* >/mnt/raid2/zlj/cmt_inc_data_$ds

hadoop fs -put  /mnt/raid2/zlj/cmt_inc_data_$ds  /hive/warehouse/wlbase_dev.db/t_base_ec_record_dev/

rm  /mnt/raid2/zlj/cmt_inc_data_$ds