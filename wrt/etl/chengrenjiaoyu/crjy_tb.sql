create table wlservice.t_wrt_tianxing_crjy_userid_loc
select t2.user_id,split(t2.tel_loc,' ')[1]
(select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20161202 and title like "%自考%")t1
JOIN
(
select a.user_id,a.item_id,b.tel_loc from
(select user_id,item_id from wlbase_dev.t_base_ec_item_feed_dev_new where ds > 20160900)a
JOIN
(select tb_id,tel_loc from wlbase_dev.t_base_user_profile where
split(tel_loc,' ')[1] in ('上海','北京','广州','深圳','南京','无锡','苏州','杭州','武汉','宁波','东莞','常州',
'珠海','佛山','江门','中山','惠州','肇庆','扬州','镇江','泰州','无锡','常州','南通','湖州','嘉兴','舟山','绍兴','台州')
)b
on
a.user_id = b.tb_id
) t2
ON
t1.item_id = t2.item
group by
t2.user_id,split(t2.tel_loc,' ')[1]
