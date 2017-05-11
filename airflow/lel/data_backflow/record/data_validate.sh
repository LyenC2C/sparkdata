#!/bin/bash
source ~/.bashrc

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
insert overwrite table wl_analysis.t_lel_record_data_backflow_validated
select * from wl_analysis.t_lel_record_data_backflow
where
(name not regexp '[A-Za-z\\d]' or name is null or name = '')
and
(length(phone) =11 or phone is null or phone = '')
and
(length(idcard) = 18  or idcard is null or idcard = '')
and
(length(idbank) = 16 or length(idbank) = 19 or idbank is null or idbank = '');
EOF


