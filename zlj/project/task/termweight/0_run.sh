sh 1_feed_item_join_corpus.sql

sh 2_count_word.sql

hadoop fs -rmr /user/zlj/tmp/data/ds

spark-commit   3_clean_fliter_corpus.py

sh  4_group_corpus.sql
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 12g  tfidf_new.py  -new  1  40  20151226  t_zlj_userbuy_item_tfidf_tagbrand_weight_2015_v2