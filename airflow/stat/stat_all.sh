#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
path=/home/airflow/sparkdata/airflow/stat

echo $today >> ~/stat/stat.log

bash $path/static_stat_impala.sh wl_base t_base_ec_xianyu_iteminfo >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_shopitem_b >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_shopitem_c >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_item_sold_dev >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_item_daysale_dev_new >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_xianyu_itemcomment >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_item_dev_new_parquet >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_ec_shop_dev_new >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_wrt_shixin_qiye >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_wrt_shixin_person >> ~/stat/stat.log
bash $path/static_stat_impala.sh wl_base t_base_credit_ppd_listinfo >> ~/stat/stat.log
