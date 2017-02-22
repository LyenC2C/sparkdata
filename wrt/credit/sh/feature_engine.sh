#!/usr/bin/env bash
source ~/.bashrc
zlj_path='/home/wrt/sparkdata/zlj/project/task/credit_model/feature'
wrt_path='/home/wrt/sparkdata/wrt/credit/feature_dense_sparse_merge'
today=$(date -d '0 days ago' +%Y%m%d)
#
#hive -f $zlj_path/cate_month_cross/t_credit_record_cate1_feature_months.sql
#hive -f $zlj_path/cate_month_cross/t_credit_record_cate2_feature_months.sql
#hive -f $zlj_path/cate_month_cross/t_credit_record_cate3_feature_months.sql
#hive -f $zlj_path/cate_month_cross/t_credit_record_cate4_feature_months.sql
#
#hive -f $zlj_path/dev/t_credit_record_feature.sql
#hive -f $zlj_path/dev/t_credit_record_feature_1month.sql
#hive -f $zlj_path/dev/t_credit_record_feature_3month.sql
#hive -f $zlj_path/dev/t_credit_record_feature_6month.sql
#hive -f $zlj_path/dev/t_credit_record_feature_12month.sql
#
#hive -f $wrt_path/feature_dense_merge.sql
#hive -f $wrt_path/feature_sparse_merge.sql

hfs -rmr /user/wrt/temp/all_features_name
hfs -rmr /user/wrt/temp/all_feature_dense_sparse_inhive

spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$wrt_path/feature_dense_sparse.py $today

#hfs -rmr /user/wrt/temp/add_new_feature_name
#hfs -rmr /user/wrt/temp/add_newfeature_inhive
#
#spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
#add_new_feat.py $today

