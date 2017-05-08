#!/bin/bash
#统计队列示例：
source ~/.bashrc

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
insert overwrite table wl_service.t_lel_record_backflow_multi_fields_standard_res
select * from
(
select *,"贷款类" as cate from
(select phone,idbank,idcard,name,max(to_date(\`date\`)) as latest, count(1)as times from wl_service.t_lel_record_data_backflow_combined  where cate = '贷款类' group by phone,idbank,idcard,name)a
union all
select *,"征信类" as cate from
(select phone,idbank,idcard,name,max(to_date(\`date\`)) as latest, count(1)as times from wl_service.t_lel_record_data_backflow_combined  where cate = '征信类' group by phone,idbank,idcard,name)b
)c;
EOF


