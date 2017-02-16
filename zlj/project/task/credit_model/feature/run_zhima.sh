
hive -f cate_month_cross/t_credit_record_cate1_feature_months.sql
hive -f cate_month_cross/t_credit_record_cate2_feature_months.sql
hive -f cate_month_cross/t_credit_record_cate3_feature_months.sql
hive -f cate_month_cross/t_credit_record_cate4_feature_months.sql

hive -f dev/t_credit_record_feature.sql
hive -f dev/t_credit_record_feature_1month.sql
hive -f dev/t_credit_record_feature_3month.sql
hive -f dev/t_credit_record_feature_6month.sql
hive -f dev/t_credit_record_feature_12month.sql