
-- 包含所有类目



Drop table t_base_record_cate ;
create table t_base_record_cate as
SELECT
  t2.*,
  t1.title,
  cat_id,
  root_cat_id,
  root_cat_name,
  cate_level1_id,
  cate_level2_id,
  cate_level3_id,
  cate_level4_id,
  cate_level5_id,
  brand_id,
  brand_name,
  bc_type,
  price,
  shop_id,
  location,
  t3.tel_index ,
  t3.rn as tel_user_rn,
  CASE WHEN cat_id IS NOT NULL
    THEN 'true'
  ELSE 'false' END AS ds
FROM (
       SELECT
        cast(item_id AS BIGINT) item_id,
        title,
        cat_id,
        root_cat_id,
        root_cat_name,
        cate_level1_id,
        cate_level2_id,
        cate_level3_id,
        cate_level4_id,
        cate_level5_id,
        brand_id,
        brand_name,
        bc_type,
        price,
        shop_id,
        location
      FROM t_base_ec_item_dev_new t1
      join t_base_ec_dim t2
      on t1.ds = 20161104  and  t1.cat_id =t2.cate_id
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
   FROM t_base_ec_item_feed_dev_new
        WHERE    item_id IS NOT NULL AND f_date IS NOT NULL
  ) t2 ON t1.item_id = t2.item_id
  left join t_zlj_phone_rank_index t3 on t2.user_id=t3.tb_id
;
