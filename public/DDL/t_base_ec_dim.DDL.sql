CREATE  TABLE  if not exists t_base_ec_dim (
cate_id         					 bigint     COMMENT '类目id                                    ',
cate_name       					 string     COMMENT '类目名称                                ',
cate_level      					 bigint     COMMENT '类目层级                                ',
cate_level1_id  					 bigint     COMMENT '一级类目id                              ',
cate_level2_id  					 bigint     COMMENT '二级类目id                              ',
cate_level3_id  					 bigint     COMMENT '三级类目id                              ',
cate_level4_id  					 bigint     COMMENT '四级类目id                              ',
cate_level5_id  					 bigint     COMMENT '五级类目id                              ',
cate_level1_name					 string     COMMENT '一级类目名称                          ',
cate_level2_name					 string     COMMENT '二级类目名称                          ',
cate_level3_name					 string     COMMENT '三级类目名称                          ',
cate_level4_name					 string     COMMENT '四级类目名称                          ',
cate_level5_name					 string     COMMENT '五级类目名称                          ',
cate_full_name  					 string     COMMENT '类目全路径                             ',
is_leaf         					 string     COMMENT '是否叶子类目                          ',
parent_id       					 bigint     COMMENT '父类目ID                                 ',
parent_name     					 string     COMMENT '父类目名称                             ',
features        					 string     COMMENT '类目特征                                ',
is_virtual      					 string     COMMENT '是否虚拟类目                          ',
properties      					 string     COMMENT '类目属性                                ',
commodity_id    					 bigint     COMMENT '品类ID                                    ',
commodity_name  					 string     COMMENT '品类名称                                ',
deleted         					 string     COMMENT '是否删除，0:未删除；1：删除；2：屏蔽 ',
cate_type       					 bigint     COMMENT '市场信息，按位取：0=缺省，1=淘宝，2=天猫 ',
industry_id     					 string     COMMENT '老的行业id                              ',
industry_name   					 string     COMMENT '老的行业名称                          ',
is_industry_valid 					 string     COMMENT ' 行业是否有效 Y:有效，N：失效    ',
industry_360_id 				 	string     COMMENT '360行业id                                 ',
industry_360_name 					string     COMMENT ' 360行业名称                             ',
is_industry_360_valid				string     COMMENT ' 360行业是否有效 Y:有效，N：失效 ',
industry_his_id 					string     COMMENT ' 历史行业id                              ',
industry_his_name 					string     COMMENT ' 历史行业名称                          ',
is_industry_his_valid				string     COMMENT ' 历史行业是否有效 Y:有效，N：失效 ',
stat_date       					string      COMMENT ' 格式：YYYYMMDD                           '
)
COMMENT '电商类目数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;