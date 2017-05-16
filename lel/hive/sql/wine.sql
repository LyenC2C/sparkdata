1.baijiu sample:
create table if not exists wlservice.t_lel_baijiu_stat_20170227 
as select user_id,item_id,dsn,title,brand_name,price,location,root_cat_name 
from t_base_ec_record_dev_new 
where ds ='true'and cat_id in (50013052,50008144) and cast(dsn as int) > 20160301 

create table if not exists t_lel_baijiu_stat_cq_20170227
as
select t2.*,t1.tel_loc from
(select /* +mapjoin(t_base_user_profile_telindex)*/ tb_id,tel_loc from wlbase_dev.t_base_user_profile_telindex where tel_loc regexp '重庆')t1
join
(select * from wlservice.t_lel_baijiu_stat_20170227) t2
on t1.tb_id = t2.user_id

create table if not exists t_lel_baijiu_stat_cq_latest_20170227
as
select 
t.user_id, t.item_id, t.dsn, t.title, t.brand_name, t.price, t.location,root_cat_name
from
(select 
user_id, item_id, dsn, title, brand_name, price, location,
root_cat_name,row_number() over(distribute by user_id,item_id sort by dsn desc) rn from  t_lel_baijiu_stat_cq_20170227)t
where t.rn=1

create table if not exists t_lel_baijiu_stat_cq_latest_one_20170227
as	
select 
t.user_id, t.item_id, t.dsn, t.title, t.brand_name, t.price, t.location,root_cat_name
from
(select 
user_id, item_id, dsn, title, brand_name, price, location,
root_cat_name,row_number() over(distribute by user_id sort by dsn desc) rn from  t_lel_baijiu_stat_cq_20170227)t
where t.rn=1

select concat(substring(tel,0,3),"********") as tel, item_id, dsn , title , brand_name , price, location, root_cat_name from  t_lel_baijiu_stat_cq_latest_20170227_product limit 150



2.提取重庆地区用户购买了茅台、五粮液、洋河、泸州老窖四个品牌中的一个，近一年购买频次1次以上，总消费金额高于200的用户的详细列表。如图示输出（将所有号码最后8位*隐藏标识）

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
create table t_lel_baijiu_stat_cq_4brands_1yearslatest_20170308
as
select 
user_id,
concat_ws(",",collect_set(regexp_extract(brand_name,'茅台|五粮液|洋河|泸州老窖',0))) as brands,
cast(sum(price) as int) as total,
size(collect_list(user_id)) as buy_times 
from t_lel_baijiu_stat_cq_20170227 
where brand_name regexp '茅台|五粮液|洋河|泸州老窖'  and cast(dsn as int ) > 20160301 
group by user_id
having cast(sum(price) as int) > 200 and size(collect_list(user_id)) >1

set hive.support.quoted.identifiers=none;
create table t_lel_baijiu_stat_cq_4brands_1yearslatest_20170308_res
as
select `(user_id|tb_id|phone)?+.+`,concat(substring(t2.phone,0,3),"********") as phone
from
(select * from t_lel_baijiu_stat_cq_4brands_1yearslatest_20170308)t1
join
(select phone,tb_id from t_lel_baijiu_stat_cq_4brands_1yearslatest_20170308_withphone)t2
on t1.user_id = t2.tb_id

3.重庆地区的用户对茅台、五粮液、洋河、泸州老窖四个品牌的正负面评价分析
create table t_lel_baijiu_stat_cq_brand_p_20170308
as
select regexp_extract(t3.brand_name,'茅台|五粮液|洋河|泸州老窖',0) as brand_name,size(collect_list(t4.content))as positive
from
(select t1.user_id,t2.item_id,t2.feed_id,t1.brand_name
from
(select user_id,item_id,brand_name from t_lel_baijiu_stat_cq_20170227)t1
join
(select user_id,item_id,feed_id from t_lel_baijiu_stat_20170227_with_itemuserfeedid)t2
on t1.user_id=t2.user_id and t1.item_id=t2.item_id)t3
join
(select user_id,item_id,feed_id,content from wl_base.t_base_ec_item_feed_dev_inc_new where content not regexp "不好|差")t4
on t3.user_id=t4.user_id and t3.item_id=t4.item_id and t3.feed_id=t4.feed_id
where brand_name regexp '茅台|五粮液|洋河|泸州老窖'
group by regexp_extract(t3.brand_name,'茅台|五粮液|洋河|泸州老窖',0)


4.重庆地区购买白酒用户区域分布
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
create table t_lel_baijiu_stat_cq_area_distribute_20170308
as
select tel_loc,size(collect_set(user_id)) as user_counts from t_lel_baijiu_stat_cq_20170227
group by tel_loc

5.
重庆地区用户购买白酒品牌分布
create table t_lel_baijiu_stat_cq_brand_distribute_20170308
as
select brand_name,size(collect_set(user_id)) as user_counts from t_lel_baijiu_stat_cq_20170227
group by brand_name sort by user_counts desc
重庆地区用户购买白酒（单瓶价格）价格分布
create table t_lel_baijiu_stat_cq_price_distribute_20170308
as
select
case when price <= 50 then "50及以下"
when price > 50 and price <= 150 then "50-150"
when price >150 and price <= 300 then "150-300"
when price >300 and price <= 600 then "300-600"
when price >600 and price <= 1000 then "600-1000"
when price >1000 then "1000以上" end as price_interval,size(collect_list(price)) as price_counts from t_lel_baijiu_stat_cq_20170227
group by case when price <= 50 then "50及以下"
when price > 50 and price <= 150 then "50-150"
when price >150 and price <= 300 then "150-300"
when price >300 and price <= 600 then "300-600"
when price >600 and price <= 1000 then "600-1000"
when price >1000 then "1000以上" end
重庆地区用户购买白酒香型分布
create table t_lel_baijiu_stat_cq_brand_xiangxin_distribute_20170308
as
select regexp_extract(title,'\\S香型',0) as xiangxin,size(collect_list(regexp_extract(title,'\\S香型',0))) as counts from t_lel_baijiu_stat_cq_20170227 where title regexp "香型"
group by regexp_extract(title,'\\S香型',0) sort by counts desc
重庆地区用户购买白酒度数分布
create table t_lel_baijiu_stat_cq_dushu_distribute_20170308
as
select 
regexp_extract(title,'[0-9]{1,2}度|[0-9]{1,2}\\.[0-9]{1,2}度',0) as dushu,
size(collect_list(regexp_extract(title,'[0-9]{1,2}度|[0-9]{1,2}\\.[0-9]{1,2}度',0))) as dushu_counts
from t_lel_baijiu_stat_cq_20170227 where regexp_extract(title,'[0-9]{1,2}度|[0-9]{1,2}\\.[0-9]{1,2}度',0) <> ''
group by regexp_extract(title,'[0-9]{1,2}度|[0-9]{1,2}\\.[0-9]{1,2}度',0)
order by cast(dushu_counts as int) desc  

红酒:

create table if not exists wl_service.t_lel_hongjiu_stat_20170313
as select user_id,item_id,dsn,title,feed_id,brand_name,price,`location`,root_cat_name 
from wl_base.t_base_ec_record_dev_new 
where ds ='true'and cat_id in ('50013003','50008143','50013004','50512003') and cast(dsn as int) > 20160301

create table if not exists t_lel_hongjiu_stat_cq_20170313
as
select t2.*,t1.tel_loc from
(select tb_id,tel_loc from wl_base.t_base_user_profile_telindex where tel_loc regexp '重庆')t1
join
(select * from wl_service.t_lel_hongjiu_stat_20170313) t2
on t1.tb_id = t2.user_id

1.重庆地区用户购买红酒（单瓶价格）价格分布

create table t_lel_hongjiu_stat_cq_price_distribute_20170313
as
select
case when price <= 50 then "50及以下"
when price > 50 and price <= 150 then "50-150"
when price >150 and price <= 300 then "150-300"
when price >300 and price <= 600 then "300-600"
when price >600 and price <= 1000 then "600-1000"
when price >1000 then "1000以上" end as price_interval,size(collect_list(price)) as price_counts from t_lel_hongjiu_stat_cq_20170313
group by case when price <= 50 then "50及以下"
when price > 50 and price <= 150 then "50-150"
when price >150 and price <= 300 then "150-300"
when price >300 and price <= 600 then "300-600"
when price >600 and price <= 1000 then "600-1000"
when price >1000 then "1000以上" end

2.重庆地区2016年4月到2017年3月的逐月销量和客单价

create table t_lel_hongjiu_stat_cq_sale_20170313
as
SELECT substring(dsn,0,6) as month,size(collect_list(price)) as sale,sum(price)/size(collect_list(user_id)) as price FROM t_lel_hongjiu_stat_cq_20170313 group by substring(dsn,0,6)


                                          



1. 整体数据集的具体时间范围、整体订单量、金额、消费者人数等麻烦帮忙明确一下。
2. 关于度数分布，数量的具体含义是什么呢，最好可以是消费者数量。   
3. 价格分布，统计价格是按单瓶、单件商品还是单个订单，同时对应的数量是瓶数、商品件数或是订单数  
4. 品牌分布的数量含义。 
5. 区域分布为什么只有三个下属区数据呢，数量的具体含义。
6. 评论分析可否针对各品牌各提取5个高频词汇。
7. 香型分布的数量含义。


1. 整体数据集的具体时间范围:近一年
   整体订单量：
   金额：
   消费者人数：
2. 关于度数分布，数量的具体含义是什么呢，最好可以是消费者数量。
   订单数
3. 价格分布，统计价格是按单瓶、单件商品还是单个订单，同时对应的数量是瓶数、商品件数或是订单数  
   单个订单  都有可能
4. 品牌分布的数量含义。
   品牌  消费者数量
5. 区域分布为什么只有三个下属区数据呢，数量的具体含义。
   只有三个

6. 评论分析可否针对各品牌各提取5个高频词汇。
   否

7. 香型分布的数量含义。
   香型  订单数量









