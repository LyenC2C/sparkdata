create table wlservice.t_wrt_tmp_shuang11_iteminfo
(
item_id string,
title string,
cat_id string,
cat_name string,
root_cat_id string,
root_cat_name string,
shopId string,
shopTitle string,
desc_score string,
service_score string,
wuliu_score string,
desc_highGap string,
service_highGap string,
wuliu_highGap string,
ts string
)
COMMENT '双11商品信息表';


create table wlservice.t_wrt_tmp_shuang11_iteminfo_new
(
item_id string,
title string,
cat_id string,
cat_name string,
root_cat_id string,
root_cat_name string,
shopId string,
shopTitle string,
desc_score string,
service_score string,
wuliu_score string,
desc_highGap string,
service_highGap string,
wuliu_highGap string,
location string,
ts string
)
COMMENT '双11商品信息表';