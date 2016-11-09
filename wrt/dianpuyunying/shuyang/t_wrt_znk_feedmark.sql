use wlervice;
CREATE EXTERNAL TABLE  if not exists t_wrt_znk_feedmark (
feed_id String COMMENT '评论id',
item_id String COMMENT '商品id',
usermark String COMMENT 'usermark',
dsn String COMMENT '评论日期'
)
COMMENT '纸尿裤评论中转表';


LOAD DATA  INPATH '/user/wrt/temp/znk_feedmark_tmp' OVERWRITE INTO TABLE t_wrt_znk_feedmark;

