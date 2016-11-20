--
create table t_wrt_tmp_canyu_shuang11 as
select item_id from
(
select
t1.item_id,
case
when (t2.saleprice/t1.max_price < 0.9) then "1"
else "0"
end as jj
from
(select item_id,max(saleprice) as max_price from t_base_ec_shopitem_b
where ds > 20161001 and ds < 20161111 group by item_id)t1
JOIN
(select item_id,saleprice  from t_base_ec_shopitem_b where ds = 20161111)t2
ON
t1.item_id=t2.item_id
)t
where jj = "1";




--1、电商数据价值指数波动价格  指数=天猫所有商品价格平均值
--无0销量
select ds,avg(saleprice) from t_base_ec_shopitem_b where sold<>0 and saleprice < 200000 group by ds;
--所有商品
select ds,avg(saleprice) from t_base_ec_shopitem_b group by ds;

--2.一级品类价格指数波动情况
--无0销量
select avg(saleprice),t1.ds,t2.root_cat_name from
(select * from t_base_ec_shopitem_b where sold<>0 and saleprice < 200000)t1
join
wlservice.t_wrt_tmp_shuang11_iteminfo t2
ON
t1.item_id = t2.item_id
group by t1.ds,t2.root_cat_name;
--所有商品
select avg(saleprice),t1.ds,t2.root_cat_name from
(select * from t_base_ec_shopitem_b where saleprice < 200000)t1
join
wlservice.t_wrt_tmp_shuang11_iteminfo t2
ON
t1.item_id = t2.item_id
group by t1.ds,t2.root_cat_name;

--3.
--3.1
select count(1) from
(
select t2.item_id,
case when t1.min_price = t2.saleprice then "1"
else "0"
end as dj
  from
(select item_id, min(saleprice)as min_price from  t_base_ec_shopitem_b
 where ds < 20161111 group by item_id)t1
JOIN
(select item_id, saleprice from t_base_ec_shopitem_b where ds = 20161111)t2
ON
t1.item_id = t2.item_id
)t
where dj = "1"
--3.2
select count(1) from t_base_ec_shopitem_b where ds = 20161111


--4.
--4.1
select root_cat_name,count(1) from
(
select t2.item_id,
t2.root_cat_name,
case when t1.min_price = t2.saleprice then "1"
else "0"
end as dj
  from
(select item_id, min(saleprice)as min_price from  t_base_ec_shopitem_b
 where ds < 20161111 group by item_id)t1
JOIN
(
select a.item_id, a.saleprice,b.root_cat_name from
(select * from t_base_ec_shopitem_b where ds = 20161111)a
JOIN
wlservice.t_wrt_tmp_shuang11_iteminfo b
ON
a.item_id = b.item_id
)t2
ON
t1.item_id = t2.item_id
)t
where dj = "1"
group by root_cat_name
--4.2
select b.root_cat_name as cat,count(1) as num from
(select * from t_base_ec_shopitem_b where ds = 20161111)a
JOIN
wlservice.t_wrt_tmp_shuang11_iteminfo b
ON
a.item_id = b.item_id
)t
group by b.root_cat_name

--5
--5.1
select root_cat_name,avg(zhangfu) as cat_zhangfu from
(
select
tt1.item_id,tt2.root_cat_name,(max(tt1.saleprice)-avg(tt1.saleprice))/avg(tt1.saleprice) as zhangfu from
(select item_id,saleprice from t_base_ec_shopitem_b where ds < 20161111)tt1
JOIN
(
select t1.item_id,t2.root_cat_name from
wlservice.t_wrt_tmp_canyu_shuang11 t1
JOIN
wlservice.t_wrt_tmp_shuang11_iteminfo t2
ON
t1.item_id=t2.item_id
)tt2
ON
tt1.item_id = tt2.item_id
group by tt1.item_id,tt2.root_cat_name
)t
group by root_cat_name

--5.2
select root_cat_name,avg(jiangfu) as cat_jiangfu from
(
select
tt1.item_id,tt2.root_cat_name,(avg(tt1.saleprice) - max(tt1.11_price))/avg(tt1.saleprice) as jiangfu from
(
select t1.item_id,t1.saleprice,t2.11_price from
(select item_id,saleprice from t_base_ec_shopitem_b where ds < 20161111)t1
join
(select item_id,saleprice as 11_price from t_base_ec_shopitem_b where ds = 20161111)t2
ON
t1.item_id = t2.item_id
)tt1
JOIN
(
select t1.item_id,t2.root_cat_name from
wlservice.t_wrt_tmp_canyu_shuang11 t1
JOIN
wlservice.t_wrt_tmp_shuang11_iteminfo t2
ON
t1.item_id=t2.item_id
)tt2
ON
tt1.item_id = tt2.item_id
group by tt1.item_id,tt2.root_cat_name
)t
group by root_cat_name;

--6
--6.1
select sum(day_sold) as sold ,sum(day_sold_price) as  sales from
(select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev_new where ds = 20151111)t1
JOIN
(select item_id from t_base_ec_item_dev_new where ds = 20161104 and bc_type = 'B')t2
ON
t1.item_id = t2.item_id

--6.2
select sum((t1.sold - t2.sold)) as sold, sum((t1.sold - t2.sold) * t1.saleprice) as sales from
(select item_id,sold,saleprice from t_base_ec_shopitem_b where ds = 20161111)t1
JOIN
(select item_id,sold from t_base_ec_shopitem_b where ds = 20161110)t2
ON
t1.item_id = t2.item_id



--7
--7.1
select root_cat_name,sum(day_sold) as sold ,sum(day_sold_price) as  sales from
(select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev_new where ds = 20151111)t1
JOIN
(select item_id,root_cat_name from t_base_ec_item_dev_new where ds = 20161104 and bc_type = 'B')t2
ON
t1.item_id = t2.item_id
group by root_cat_name
--7.2

select tt2.root_cat_name, sum(tt1.sold)as sold, sum(tt1.sales) as sales from
(
select t1.item_id,(t1.sold - t2.sold) as sold, (t1.sold - t2.sold) * t1.saleprice as sales from
(select item_id,sold,saleprice from t_base_ec_shopitem_b where ds = 20161111)t1
JOIN
(select item_id,sold from t_base_ec_shopitem_b where ds = 20161110)t2
ON
t1.item_id = t2.item_id
)tt1
JOIN
wlservice.t_wrt_tmp_shuang11_iteminfo tt2
ON
tt1.item_id = tt2.item_id
group by tt2.root_cat_name
--8

select
cast(((cast(desc_highgap as float) + cast(service_highgap as float) + cast(wuliu_highgap as float)
+ 279.260009765625) / 57.926000976562499)
as int ) as point,count(1)
from
(select shopid,max(desc_highgap) as desc_highgap,max(service_highgap) as service_highgap ,max(wuliu_highgap) as wuliu_highgap
from wlservice.t_wrt_tmp_shuang11_iteminfo
group by shopid
)t
group by
cast(((cast(desc_highgap as float) + cast(service_highgap as float) + cast(wuliu_highgap as float)
+ 279.260009765625) / 57.926000976562499)
as int )