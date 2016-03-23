create table t_wrt_item_content_datatao(
item_id STRING,
content STRING
);


insert overwrite table t_wrt_item_content_datatao
select t.item_id,t.content from
t_wrt_iteminfo_datatao
join
(select item_id,content from t_base_ec_item_feed_dev where ds >= 20151101 and ds<= 20160131)t
on
t_wrt_iteminfo_datatao.item_id = t.item_id;

--python ../content.py