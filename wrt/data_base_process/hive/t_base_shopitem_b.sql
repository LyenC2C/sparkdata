ds=$1

hive<<EOF

use wlbase_dev;

LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_b PARTITION (ds='$ds');

EOF