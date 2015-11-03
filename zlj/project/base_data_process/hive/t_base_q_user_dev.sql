

set hive.exec.reducers.bytes.per.reducer=500000000;
use wlbase_dev;

LOAD DATA  INPATH '/user/zlj/data/qqinfo' OVERWRITE INTO TABLE t_base_q_user_dev PARTITION (ds='20151023');
