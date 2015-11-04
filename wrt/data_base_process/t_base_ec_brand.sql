use wlbase_dev;

LOAD DATA  INPATH '/user/wrt/result' OVERWRITE INTO TABLE t_base_ec_brand PARTITION (ds='20151103');