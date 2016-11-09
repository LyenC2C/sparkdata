CREATE EXTERNAL TABLE  if not exists t_wrt_tmallint_iteminfo(
    item_id String,
    title String,
    categoryId String,
    cate_name String,
    cate_root_name String,
    brandId String,
    brand_name String,
    price String,
    price_zone String,
    favor String,
    seller_id String,
    shopId String,
    country String,
    location String,
    ts String
)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

CREATE EXTERNAL TABLE  if not exists t_wrt_tmallint_nocountry(
    item_id String,
    title String,
    categoryId String,
    cate_name String,
    cate_root_name String,
    brandId String,
    brand_name String,
    price String,
    price_zone String,
    favor String,
    seller_id String,
    shopId String,
    country String,
    location String,
    ts String
)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

CREATE EXTERNAL TABLE  if not exists t_wrt_tmallint_nocountry(
    item_id String,
    title String,
    categoryId String,
    cate_name String,
    cate_root_name String,
    brandId String,
    brand_name String,
    price String,
    price_zone String,
    favor String,
    seller_id String,
    shopId String,
    country String,
    location String,
    ts String
)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

CREATE EXTERNAL TABLE  if not exists t_wrt_tmp_tmallint_notin_nocountry(
    item_id String,
    title String,
    categoryId String,
    cate_name String,
    cate_root_name String,
    brandId String,
    brand_name String,
    price String,
    price_zone String,
    favor String,
    seller_id String,
    shopId String,
    country String,
    location String,
    ts String
)
COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;



insert overwrite table t_wrt_tmallint_iteminfo
select
t1.item_id,
t1.title,
t1.categoryId,
t1.cate_name,
t1.cate_root_name,
t1.brandId,
t1.brand_name,
t1.price,
t1.price_zone,
t1.favor,
t1.seller_id,
t1.shopId,
t2.country,
t1.location,
t1.ts
FROM
(select * from t_wrt_korea_iteminfo)t1
JOIN
(select brand_id,max(country) as country from t_wrt_brandid_country group by brand_id)t2
ON
t1.brandId = t2.brand_id;




insert into table t_wrt_tmallint_iteminfo
select * from t_wrt_korea_iteminfo where country <> '未知' and country <> '-' and country <> ' ' and country <> '';

select count(1) from(
select brandId from t_wrt_tmallint_iteminfo group by brandId)t

select count(1) from(
select brandId from t_wrt_korea_iteminfo group by brandId)t

insert overwrite table t_wrt_tmallint_nocountry
select *
from t_wrt_korea_iteminfo
where country= '未知' or country= '-' or country= ' ' or country= ''

select count(1) from
(
select brandid
from t_wrt_korea_iteminfo
where country= '未知' or country= '-' or country= ' ' or country= ''
group by brandid
)t

select count(1) FROM
(
select item_id from t_wrt_tmallint_nocountry group by item_id
)t

insert overwrite table t_wrt_tmp_tmallint_notin_nocountry
select
t1.item_id,
t1.title,
t1.categoryId,
t1.cate_name,
t1.cate_root_name,
t1.brandId,
t1.brand_name,
t1.price,
t1.price_zone,
t1.favor,
t1.seller_id,
t1.shopId,
t1.country,
t1.location,
t1.ts
from
(select * from t_wrt_tmallint_nocountry)t1
left JOIN
(select * from t_wrt_tmallint_iteminfo)t2
ON
t1.item_id = t2.item_id
WHERE
t2.item_id is NULL;

select t1.brandid from
(select brandid from t_wrt_tmp_tmallint_notin_nocountry group by brandid)t1
JOIN
(select brandid from t_wrt_tmallint_iteminfo group by brandid)t2
ON
t1.brandid = t2.brandid;


create table t_wrt_tmp_gangid
(
item_id String
)

insert overwrite table t_wrt_tmp_gangid
select item_id from t_wrt_tmp_tmallint_notin_nocountry where brandid = '-'

t_wrt_tmp_gangid