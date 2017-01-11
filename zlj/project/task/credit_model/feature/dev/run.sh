
#hive -f t_credit_record_cate1_feature.sql
#hive -f t_credit_record_cate2_feature.sql
#hive -f t_credit_record_cate3_feature.sql
#hive -f t_credit_record_cate4_feature.sql
#
#hive -f t_credit_record_feature.sql
hive -f t_credit_record_feature_1month.sql
hive -f t_credit_record_feature_3month.sql
hive -f t_credit_record_feature_6month.sql
hive -f t_credit_record_feature_12month.sql