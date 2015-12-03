spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_家居家纺.dat.tar.gz  \
/user/zlj/baifendian.data/data_1205/pageview_家居家纺.dat.tar.gz  /user/zlj/wrt/search/jiaju_keyword_itemtitle

spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_美食特产.dat.tar.gz  \
/user/zlj/baifendian.data/data_1205/pageview_美食特产.dat.tar.gz  /user/zlj/wrt/search/meishi_keyword_itemtitle

spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_riyongbaihuo.dat.tar.gz  \
/user/zlj/baifendian.data/data_1205/pageview_riyongbaihuo.dat.tar.gz  /user/zlj/wrt/search/riyong_keyword_itemtitle

spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_医疗保健.dat.tar.gz  \
/user/zlj/baifendian.data/data_1205/pageview_医疗保健.dat.tar.gz /user/zlj/wrt/search/yiliao_keyword_itemtitle

spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_运动户外.dat.tar.gz  \
/user/zlj/baifendian.data/data_1205/pageview_运动户外.dat.tar.gz  /user/zlj/wrt/search/huwai_keyword_itemtitle

spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_团购.dat.tar.gz  \
/user/zlj/baifendian.data/data_1205/pageview_团购.dat.tar.gz /user/zlj/wrt/search/tuangou_keyword_itemtitle

spark-submit  --executor-memory 16G  --driver-memory 20G  --total-executor-cores 160 pre_process_spark.py \
/user/wrt/cat_map/ /user/zlj/baifendian.data/data_1205/item_电脑办公.dat.tar.gz \
/user/zlj/baifendian.data/data_1205/pageview_电脑办公.dat.tar.gz /user/zlj/wrt/search/diannao_keyword_itemtitle



