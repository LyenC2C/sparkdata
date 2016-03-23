
create table t_wrt_brandcount_datatao(
brand_id String,
brand_count String,
brand_name String
);

insert overwrite table t_wrt_brandcount_datatao
select tt1.brand_id,count(1) as brand_count, tt1.brand_name from
(select brand_id,max(brand_name) as brand_name from
t_base_ec_item_dev where ds = 20160225 and root_cat_id = 1801 and bc_type = 'B' and brand_id <> '-'
group by brand_id limit 100)tt1
join
(select brand_id from t_base_ec_item_dev where ds = 20160225 and brand_id <> '-' and bc_type = 'B')tt2
on
tt1.brand_id = tt2.brand_id
group by tt1.brand_id,tt1.brand_name;