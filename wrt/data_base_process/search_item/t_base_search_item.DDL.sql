crEATE  TABLE  if not exists t_base_ec_search_item (
item_id string comment '商品id',
comment_count string comment '评论数量'
)
COMMENT '电商搜索商品id与评论数量'
PARTITIONED BY  (ds STRING );
