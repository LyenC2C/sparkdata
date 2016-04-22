

set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;

LOAD DATA  INPATH '/user/zlj/data/qqinfo' OVERWRITE INTO TABLE t_base_q_user_dev PARTITION (ds='20151023');


LOAD DATA  INPATH '/user/zlj/data/qq_age' OVERWRITE INTO TABLE t_base_q_user_dev_zlj PARTITION (ds='20151104');

-- ALTER TABLE t_base_q_user_dev_zlj DROP IF EXISTS PARTITION (ds='20151103');
-- ALTER TABLE t_base_ec_item_feed_dev_zlj DROP IF EXISTS PARTITION (ds='20100004');