ds=$1

hive<<EOF

use wlbase_dev;

LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sale_dev PARTITION (ds='$ds');

EOF