
select count(1) from
(select item_id from t_base_ec_item_sale_dev where ds = 20160316)t1
join
(select item_id from t_base_ec_item_dev where ds = 20160225)t2
on
t1.item_id = t2.item_id;




select item_id from
(select item_id from t_base_ec_item_sale_dev where ds = 20160316)t1
left join
(select item_id from t_base_ec_item_dev where ds = 20160225)t2
on
t1.item_id = t2.item_id;
where t2.item_id is null