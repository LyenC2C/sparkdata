
create table if not exists wlservice.t_wrt_shuang11_itemid_title_price(
item_id String COMMENT '商品id',
title String COMMENT '商品名称',
saleprice float COMMENT '售价'
)
COMMENT '双11商品信息表';
