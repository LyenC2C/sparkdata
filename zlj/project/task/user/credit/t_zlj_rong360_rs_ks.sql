CREATE  TABLE  if not exists t_zlj_rong360_rs_ks (
predict        string   comment '预测',
label        string   comment '类目id',
id1      string   comment '类目名称',
id2      string   comment '类目名称',
id3      string   comment '类目名称',
id4      string   comment '类目名称',
id5      string   comment '类目名称'
)
COMMENT 'rong360'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n'
stored as textfile ;

LOAD DATA  INPATH '/user/zlj/model/rong360_v1' OVERWRITE INTO TABLE t_zlj_rong360_rs_ks PARTITION (ds='20161024') ;
LOAD DATA  INPATH '/user/zlj/model/rong360_v2' OVERWRITE INTO TABLE t_zlj_rong360_rs_ks PARTITION (ds='v2') ;
LOAD DATA  INPATH '/user/zlj/model/rong360_v3' OVERWRITE INTO TABLE t_zlj_rong360_rs_ks PARTITION (ds='v3') ;
LOAD DATA  INPATH '/user/zlj/tmp/20161027.dec.sq.phone.weibouser'
 OVERWRITE INTO TABLE t_zlj_rong360_rs_ks PARTITION (ds='phone_weibouser_1027') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;

