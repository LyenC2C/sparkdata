use wlbase_dev;
create table t_wrt_zhejiang_shop_loc AS
select shop_id, location from t_base_ec_shop_dev where substr(location,1,2) = '浙江' and bc_type = 'B' and ds = '20151101'