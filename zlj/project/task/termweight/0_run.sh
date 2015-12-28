sh 1_feed_item_join_corpus.sql

sh 2_count_word.sql

hadoop fs -rmr /user/zlj/tmp/data/ds

spark-commit   3_clean_fliter_corpus.py

