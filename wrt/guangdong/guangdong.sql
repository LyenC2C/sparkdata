2.a
select substr(location,3,2)as loc,count(1) from t_base_ec_shop_dev_new
where ds = 20160622 and substr(location,1,2) = '广东' and bc_type = 'C' group by  substr(location,3,2)


2.b
select ,sales,rn from
(
select shop_id,item_id,title,sold,sales,ROW_NUMBER() OVER (PARTITION BY shop_id ORDER BY sold desc) AS rn from
(
select item_id,shop_id,max(title) as title,cast(sum(sold) as int) as sold,sum(sales) as sales FROM
t_wrt_tmp_14shop_totalsold where ds > 20160500
group by item_id,shop_id)t
)tt
where rn <16;

create table t_wrt_guangdong_sale as
select t1.item_id,t1.shop_id,t1.loc,t1.bc_type,max(t1.root_cat_name),sum(t2.day_sold) as sold,sum(day_sold_price) as sales FROM
(select item_id,shop_id,substr(location,3,2)as loc,bc_type,root_cat_name from wlbase_dev.t_base_ec_item_dev_new
where ds = 20160621 and substr(location,1,2) = '广东')t1
join
(select * from wlbase_dev.t_base_ec_item_daysale_dev_new where ds > 20160000 and ds < 20160600)t2
ON
t1.item_id = t2.item_id
group by t1.item_id,t1.shop_id,t1.loc,t1.bc_type;



select loc,sum(sold),sum(sales) from t_wrt_guangdong_sale where bc_type = 'B' group by loc;
select loc,sum(sold),sum(sales) from t_wrt_guangdong_sale where bc_type = 'C' group by loc;


2.c
select t_t1.loc,t_t1.hangye,t_t1.sold/t_t2.total,t_t1.rn from
(
select loc,hangye,sold,rn from
  (
    select loc,hangye,sold,ROW_NUMBER() OVER (PARTITION BY loc ORDER BY sold desc) AS rn from
      (
      select loc,hangye,sum(sold) as sold from
        (
        select t1.*,t2.main_cat_name as hangye from
        t_wrt_guangdong_sale t1
        JOIN
        wlbase_dev.t_zlj_shop_join_major t2
        on
        t1.shop_id = t2.shop_id
        )t
      group by loc,hangye
      )tt1
  )ttt
where rn < 4
)t_t1
JOIN
(
  select loc,sum(sold) as total from
  (
  select t1.*,t2.main_cat_name as hangye from
  t_wrt_guangdong_sale t1
  JOIN
   wlbase_dev.t_zlj_shop_join_major t2
  on
  t1.shop_id = t2.shop_id
  )t
  group by loc
)t_t2
ON
t_t1.loc = t_t2.loc


--四川，广东，浙江
--店铺年销售额 120W-2000w，
--id，地址，店铺名:
select shop_id,loc,shop_name,sales from
(
select tt1.shop_id,tt1.loc,tt2.shop_name,cast(tt1.nian_sales as float) as sales from
(
select t1.shop_id as shop_id ,max(t1.loc) as loc,(sum(t2.day_sold_price) * 2) as nian_sales from
(
select item_id,substr(location,1,2) as loc,shop_id from wlbase_dev.t_base_ec_item_dev_new
where ds = 20160621
and (substr(location,1,2) = '广东' or
substr(location,1,2) = '四川' or
substr(location,1,2) = '浙江')
)t1
JOIN
(
select * from  wlbase_dev.t_base_ec_item_daysale_dev_new where ds > 20151200 and ds < 20160600
)t2
ON
t1.item_id = t2.item_id
group by t1.shop_id
)tt1
JOIN
(
select * from t_base_ec_shop_dev_new where ds = 20160622
)tt2
ON
tt1.shop_id = tt2.shop_id
)tdt
where sales > 1200000.0 and sales < 20000000.0


--广东跨境电商店铺数量   各品类 销量  销售额
--全国占比
select count(1) from
(select * from t_base_ec_shop_dev_new where ds = 20160622 and ds = 20160622 and substr(location,1,2) = '广东')t1
 join
(select shop_id from t_base_shop_type where shop_type['globalgou'] = 'True' or shop_type['tmhk'] = 'True')t2
ON
t1.shop_id = t2.shop_id;



(select * from wlservice.t_wrt_guangdong_sale)t1
 join
(select shop_id from t_base_shop_type where shop_type['globalgou'] = 'True' or shop_type['tmhk'] = 'True')t2
ON
t1.shop_id = t2.shop_id


--四个省的店铺id,店铺城市，sellerid ,seller name, 商品数，销量，销售额

create table t_wrt_3province_allshop_v2 as
select tt1.shop_id,tt2.shop_name,tt1.location,tt2.seller_id,tt2.seller_name,tt2.itemcount,
tt1.nian_sold,tt1.nian_sales from
(
  select t1.shop_id as shop_id ,max(t1.location) as location,
(sum(t2.day_sold) * 2) as nian_sold,
(sum(t2.day_sold_price) * 2) as nian_sales from
(
select item_id,location,shop_id from wlbase_dev.t_base_ec_item_dev_new
where ds = 20160621
and
(substr(location,1,2) = '广东' or
substr(location,1,2) = '四川' or
substr(location,1,2) = '浙江' OR
substr(location,1,2) = '山东')
)t1
JOIN
(
select * from  wlbase_dev.t_base_ec_item_daysale_dev_new where ds > 20151200 and ds < 20160600
)t2
ON
t1.item_id = t2.item_id
group by t1.shop_id
)tt1
JOIN
(
select t1.shop_id,t1.itemcount,t2.seller_id,t2.seller_name,t2.shop_name FROM
(select shop_id,count(1) as itemcount from wlbase_dev.t_base_ec_item_dev_new where ds = 20160621 group by shop_id)t1
JOIN
(select * from wlbase_dev.t_base_ec_shop_dev_new where ds = 20160622)t2
ON
t1.shop_id = t2.shop_id
)tt2
ON
tt1.shop_id = tt2.shop_id;



