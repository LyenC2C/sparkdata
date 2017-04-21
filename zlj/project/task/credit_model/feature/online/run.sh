#impala -k -i cs108 -f
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_cate1_feature_months_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_cate2_feature_months_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_cate3_feature_months_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_cate4_feature_months_online.sql

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_feature_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_feature_1month_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_feature_3month_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_feature_6month_online.sql
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM" -f t_credit_record_feature_12month_online.sql

