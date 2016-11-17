SET hive.exec.dynamic.partition= TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.reducers.bytes.per.reducer=500000000;


Drop table t_base_ec_record_dev_new_tmp;
create table t_base_ec_record_dev_new_tmp as
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
  t3.tel_index ,
  t3.rn as tel_user_rn,
  CASE WHEN cat_id IS NOT NULL
    THEN 'true1'
  ELSE 'false1' END AS ds
FROM (
       SELECT
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
      WHERE ds = 20161013
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
      case when content='评价方未及时做出评价,系统默认好评\!' then date_add(f_date,-22)
           when content='好评\！' then date_add(f_date,-11)
           else  date_add(f_date,-9)
      end  as date_predict ,
      sku
   FROM t_base_ec_item_feed_dev_inc_new
        WHERE ds='20160926' and item_id IS NOT NULL AND f_date IS NOT NULL
  ) t2 ON t1.item_id = t2.item_id
  left join t_zlj_phone_rank_index t3 on t2.user_id=t3.tb_id
;
