use wlbase_dev;

LOAD DATA  INPATH '/user/wrt/item_pinpai_result/' OVERWRITE INTO TABLE t_wrt_item_tag_level PARTITION (ds='20151108');