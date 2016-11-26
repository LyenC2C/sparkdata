/hive/warehouse/wlbase_dev.db/t_base_record_cate
/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new
/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri_bi_friends_v11
/hive/warehouse/wlbase_dev.db/t_base_uid_tmp
/hive/warehouse/wlbase_dev.db/t_base_user_profile
/hive/warehouse/wlbase_dev.db/t_base_weibo_career


create table wl_base.t_base_record_cate like wlbase_dev.t_base_record_cate ;

LOAD DATA   INPATH '/hive/warehouse/wlbase_dev.db/t_base_record_cate' OVERWRITE INTO TABLE wl_base.t_base_record_cate ;



create table wl_base.t_base_ec_record_dev_new like wlbase_dev.t_base_ec_record_dev_new ;
LOAD DATA   INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=false/' OVERWRITE INTO TABLE
wl_base.t_base_ec_record_dev_new PARTITION (ds='false') ;

LOAD DATA   INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new/ds=true/' OVERWRITE INTO TABLE
wl_base.t_base_ec_record_dev_new PARTITION (ds='true') ;


create table wl_base.t_base_weibo_user_fri_bi_friends_v11 like wlbase_dev.t_base_weibo_user_fri_bi_friends_v11 ;
LOAD DATA   INPATH '/hive/warehouse/wl_base.db/t_base_weibo_user_fri_bi_friends_v11/' OVERWRITE INTO TABLE wl_base.t_base_weibo_user_fri_bi_friends_v11 ;


create table wl_base.t_base_uid_tmp as select * from wlbase_dev.t_base_uid_tmp ;

create table wl_base.t_base_user_profile as select * from wlbase_dev.t_base_user_profile ;

create table wl_base.t_base_weibo_career as select * from wlbase_dev.t_base_weibo_career ;




/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_simple
/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri_tel
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step9
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step8
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step7
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step6
/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri_bi_friends
/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri_bi_friends_groupby_12
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step5
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step4
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_step3
/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri_bi_friends_1106_fiter_users
/hive/warehouse/wlbase_dev.db/t_base_user_profile_telindex
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_join
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t
/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo_t_20160418
/hive/warehouse/wlbase_dev.db/t_base_user_info_s
/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri_bi_tel
/hive/warehouse/wlbase_dev.db/t_base_uid
/hive/warehouse/wlbase_dev.db/t_base_weibo_blackuser
/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360
/hive/warehouse/wlbase_dev.db/t_base_shop_major_all
/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360_feature_train
/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360_feature_train_allclass
/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360_feature_zlj
/hive/warehouse/wlbase_dev.db/t_base_credit_consume_join_data_zm_data
/hive/warehouse/wlbase_dev.db/t_base_ec_brand_level



create tabel wl_base.t_base_ec_record_dev_new_simple as  select * from wlbase_dev.t_base_ec_record_dev_new_simple ;
create tabel wl_base.t_base_weibo_user_fri_tel as  select * from wlbase_dev.t_base_weibo_user_fri_tel ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step9 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step9 ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step8 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step8 ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step7 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step7 ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step6 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step6 ;
create tabel wl_base.t_base_weibo_user_fri_bi_friends as  select * from wlbase_dev.t_base_weibo_user_fri_bi_friends ;
create tabel wl_base.t_base_weibo_user_fri_bi_friends_groupby_12 as  select * from wlbase_dev.t_base_weibo_user_fri_bi_friends_groupby_12 ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step5 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step5 ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step4 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step4 ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_step3 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_step3 ;
create tabel wl_base.t_base_weibo_user_fri_bi_friends_1106_fiter_users as  select * from wlbase_dev.t_base_weibo_user_fri_bi_friends_1106_fiter_users ;
create tabel wl_base.t_base_user_profile_telindex as  select * from wlbase_dev.t_base_user_profile_telindex ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_join as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_join ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t ;
create tabel wl_base.t_base_user_info_s_tbuserinfo_t_20160418 as  select * from wlbase_dev.t_base_user_info_s_tbuserinfo_t_20160418 ;
create tabel wl_base.t_base_user_info_s as  select * from wlbase_dev.t_base_user_info_s ;
create tabel wl_base.t_base_weibo_user_fri_bi_tel as  select * from wlbase_dev.t_base_weibo_user_fri_bi_tel ;
create tabel wl_base.t_base_uid as  select * from wlbase_dev.t_base_uid ;
create tabel wl_base.t_base_weibo_blackuser as  select * from wlbase_dev.t_base_weibo_blackuser ;
create tabel wl_base.t_base_ec_record_dev_new_rong360 as  select * from wlbase_dev.t_base_ec_record_dev_new_rong360 ;
create tabel wl_base.t_base_shop_major_all as  select * from wlbase_dev.t_base_shop_major_all ;
create tabel wl_base.t_base_ec_record_dev_new_rong360_feature_train as  select * from wlbase_dev.t_base_ec_record_dev_new_rong360_feature_train ;
create tabel wl_base.t_base_ec_record_dev_new_rong360_feature_train_allclass as  select * from wlbase_dev.t_base_ec_record_dev_new_rong360_feature_train_allclass ;
create tabel wl_base.t_base_ec_record_dev_new_rong360_feature_zlj as  select * from wlbase_dev.t_base_ec_record_dev_new_rong360_feature_zlj ;
create tabel wl_base.t_base_credit_consume_join_data_zm_data as  select * from wlbase_dev.t_base_credit_consume_join_data_zm_data ;
create tabel wl_base.t_base_ec_brand_level as  select * from wlbase_dev.t_base_ec_brand_level ;
