

source /home/yarn/.bashrc
path=$1

/home/yarn/hive/bin/hive<<EOF

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;

set hive.exec.reducers.bytes.per.reducer=500000000;

use wlbase_dev;

LOAD DATA  INPATH '$path' OVERWRITE INTO TABLE t_base_ec_item_feed_dev_tmp PARTITION (ds='20000001');


-- 增量数据fetch 入库
-- INSERT overwrite TABLE t_base_ec_item_feed_dev PARTITION (ds )

INSERT INTO TABLE t_base_ec_item_feed_dev PARTITION (ds)
select
item_id,
item_title,
feed_id,
user_id,
content,
f_date ,
annoy  ,
ts ,
SUBSTRING (regexp_replace(f_date,'-',''),0,8) ds
FROM t_base_ec_item_feed_dev_tmp where ds=20000001
;

EOF

echo "rm tmp table"
hadoop fs -rmr /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_tmp/ds=20000001/*