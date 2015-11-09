
path=$1

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;

use wlbase_dev;

LOAD DATA  INPATH '$path' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_zlj PARTITION (ds='20101103');



-- ��̬����
-- INSERT overwrite TABLE t_base_ec_item_feed_dev PARTITION (ds )

INSERT INTO TABLE t_base_ec_item_feed_dev_zlj PARTITION (ds)
select
item_id,
item_title,
feed_id,
user_id,
content,
f_date ,
annoy  ,
ts ,
regexp_replace(f_date,'-','') ds
from

FROM t_base_ec_item_feed_dev_zlj where ds=20101103
;