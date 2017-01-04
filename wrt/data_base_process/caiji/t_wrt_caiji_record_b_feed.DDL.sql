create table wl_analysis.t_wrt_caiji_record_b_feed(
item_id string comment '商品id',
new_ds string comment '商品在已在库中的最新时间',
sold string comment '销量(采集次序需要)'
)
COMMENT '采集天猫评论所需的商品id'
PARTITIONED BY  (ds STRING );