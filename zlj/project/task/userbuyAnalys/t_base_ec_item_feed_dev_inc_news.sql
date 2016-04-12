

t_base_ec_record_dev_new
新记录表

 ALTER table  t_base_ec_record_dev_047_test_withnotnull   RENAME  to t_base_ec_record_dev_new ;





select sum(case when cat_id is null then 1 end ) ,sum(case when cat_id is not null then 1 end ) from t_base_ec_record_dev_047_test ;

236081411       6544727104

select sum(case when cat_id is null then 1 end ) ,sum(case when cat_id is not null then 1 end ) from t_base_ec_record_dev_047_test_withnull_join_item ;

228064119       8017292


create table t_base_ec_record_dev_new  as
select * from

(
select
* from
t_base_ec_record_dev_047_test_withnull_join_item

UNION ALL

select *
from t_base_ec_record_dev_047_test_withnotnull
)t  ;





ALTER table t_base_ec_record_dev_047_test_withnotnull RENAME  to t_base_ec_record_dev_047_test_withnull ;

create table t_base_ec_record_dev_047_test_itemid as


create table t_base_ec_record_dev_047_test_withnotnull as
select *
from
t_base_ec_record_dev_047_test

where  cat_id is not null
;


select title,brand_name  from  t_base_ec_record_dev_047_test_withnotnull where user_id =69028781


select * from t_zlj_userbuy_item_tfidf_tagbrand_weight_2015_v4 where  tfidftags like  '%十万个为什么%' ;


create table t_base_ec_record_dev_047_test_withnull_join_item
as
select
t2.item_id,
feed_id,
user_id,
content_length ,
annoy ,
ds ,
datediff ,
sku ,
t1.title,
      t1.cat_id,
      t1.root_cat_id,
      t1.root_cat_name,
      t1.brand_id,
      t1.brand_name,
      t1.bc_type,
      t1.price,
      t1.shop_id,
      t1.location
from
 (SELECT
            item_id,
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
          FROM t_base_ec_item_dev
          WHERE ds = 20160333
          ) t1
RIGHT OUTER join
t_base_ec_record_dev_047_test_withnull  t2
 ON t1.item_id = t2.item_id
 ;


create table t_base_ec_record_dev_047_test as

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
            shop_id,
            location
          FROM t_base_ec_item_dev
          WHERE ds = 20160332
          ) t1
     RIGHT OUTER JOIN
      (
        SELECT
          cast(item_id AS BIGINT)      item_id,
          feed_id,
          user_id,
          length(content)              content_length,
          annoy,
          SUBSTRING (regexp_replace(f_date,'-',''),0,8)  as ds ,
          datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), SUBSTRING (f_date,0,10)) AS datediff,
          sku

        FROM t_base_ec_item_feed_dev


      ) t2 ON t1.item_id = t2.item_id;



