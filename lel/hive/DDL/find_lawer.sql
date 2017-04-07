CREATE TABLE  if not exists wl_base.t_base_phone_find_lawer(
phone String COMMENT '律师电话',
lawer_name String COMMENT '律师名字',
practice_company String COMMENT '所在公司',
lawer_office String COMMENT '办公地点',
lvshi_info_add String COMMENT '其他信息',
lawer_phone String COMMENT '律师电话所在地',
professional_qualifications String COMMENT '营业执照',
lawer_reduce String COMMENT '口碑',
lawer_info_url String COMMENT '律师信息网址',
lawer_e_mail String COMMENT '律师email',
lawer_qq String COMMENT '律师qq',
year_of_practice String COMMENT '业龄',
img_url String COMMENT '头像链接'
)
COMMENT '律师信息搜索'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/find_lawer_search" overwrite into table wl_base.t_base_phone_find_lawer partition(ds=20170216)


CREATE TABLE  if not exists wl_base.t_base_phone_find_lawer_v2(
phone String COMMENT '律师区号+电话',
acode String comment '电话区号',
phone_num String '电话',
lawer_name String COMMENT '律师名字',
practice_company String COMMENT '所在公司',
lawer_office String COMMENT '办公地点',
lvshi_info_add String COMMENT '其他信息',
lawer_phone String COMMENT '律师电话所在地',
professional_qualifications String COMMENT '营业执照',
lawer_reduce String COMMENT '口碑',
lawer_info_url String COMMENT '律师信息网址',
lawer_e_mail String COMMENT '律师email',
lawer_qq String COMMENT '律师qq',
year_of_practice String COMMENT '业龄',
img_url String COMMENT '头像链接'
)
COMMENT '律师信息搜索'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data inpath "/user/lel/temp/find_lawer_search" overwrite into table wl_base.t_base_phone_find_lawer partition(ds=20170216)

