#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
insert overwrite table wl_service.t_lel_record_data_backflow_combined
as
select
b.company,a.phone,a.idbank,a.idcard,a.name,a.\`date\`,b.cate from
(select
app_key,\`date\`,
case when phone is null then '' else phone end as phone,
case when idbank is null then '' else idbank end as idbank,
case when idcard is null then '' else idcard end as idcard,
case when name is null then '' else name end as name
from wl_analysis.t_lel_record_data_backflow_validated)a
join
(select * from wl_service.t_lel_datamart_backflow_company_cates)b
on a.app_key=b.app_key;
EOF

