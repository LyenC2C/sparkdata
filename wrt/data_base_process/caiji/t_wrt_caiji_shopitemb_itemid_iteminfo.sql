create table wlservice.t_wrt_caiji_shopitemb_itemid_iteminfo as
select item_id from wlbase_dev.t_base_ec_shopitem_b where ds = 20161214 and (down_day > '20161115' or down_day = '0')


