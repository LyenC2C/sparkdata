create table t_wrt_baijiu_itemsale (
item_id string,
day_sold bigint,
day_sold_price float,
m string,
ds string
);

insert overwrite table t_wrt_baijiu_itemsale
select t1.item_id,t2.day_sold,t2.day_sold_price,t2.m,t2.ds from
(select item_id from t_wrt_baijiu_iteminfo)t1
JOIN
(select item_id,day_sold,day_sold_price,ds,SUBSTR(ds,5,2) as m from wlbase_dev.t_base_ec_item_daysale_dev
where ds >= 20160201 and ds <= 20160316
)t2
on t1.item_id = t2.item_id;


insert into table t_wrt_baijiu_itemsale
select t1.item_id,t2.day_sold,t2.day_sold_price,t2.m,t2.ds from
(select item_id from t_wrt_baijiu_iteminfo)t1
JOIN
(select item_id,day_sold,day_sold_price,ds,SUBSTR(ds,5,2) as m from wlbase_dev.t_base_ec_item_daysale_dev
where ds >= 20151101 and ds <= 20160131
)t2
on t1.item_id = t2.item_id;