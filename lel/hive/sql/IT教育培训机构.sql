IT教育培训机构:

create table wl_service.t_lel_it_java_20170420_bj
as
SELECT a.user_id from
(select distinct user_id
from wl_base.t_base_ec_record_dev_new where ds = 'true' and title regexp 'java.*(教程|编程|教材|程序|开发|入门|经典|精通|培训|基础|自学|学习)' and root_cat_id = '33' and cast(dsn as int) > 20160425)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '北京')b
on a.user_id = b.tb_id


create table wl_service.t_lel_it_php_20170420_bj
as
SELECT a.user_id from
(select distinct user_id
from wl_base.t_base_ec_record_dev_new where ds = 'true' and title regexp 'php.*(教程|编程|教材|程序|开发|入门|经典|精通|培训|基础|自学|学习)' and root_cat_id = '33' and cast(dsn as int) > 20160425)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '北京')b
on a.user_id = b.tb_id

create table wl_service.t_lel_it_20170420_bj
as
SELECT a.user_id from
(select distinct user_id
from wl_base.t_base_ec_record_dev_new where ds = 'true' and title regexp '软件开发|软件工程|软件设计|编程|程序|C语言|java|php' and root_cat_id = '33' and cast(dsn as int) > 20161001)a
left semi join
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '北京')b
on a.user_id = b.tb_id






t_lel_it_20170420_bj -> 6553
t_lel_it_java_20170420_bj ->940



awk -F '\001' 'NR==FNR{if($4 ~ /'北京'/)a[$2]=$0}NR!=FNR{if(substr($1,1,7) in a)print $1}' /home/lyen/下载/phone/phone_20170301 /home/lyen/下载/jap/java.teltb | sort | uniq | wc -l
awk -F '\001' 'NR==FNR{a[$2]=$0}NR!=FNR{if(substr($1,1,7) in a)print $1}' /home/lyen/下载/phone/phone_20170301 /home/lyen/下载/jap/java.teltb | sort | uniq  > java_940
awk -F '\001' 'NR==FNR{a[$2]=$0}NR!=FNR{if(substr($1,1,7) in a)print $1}' /home/lyen/下载/phone/phone_20170301 /home/lyen/下载/jap/php.teltb | sort | uniq > php_57

awk 'NR==FNR{a[$0]=$0}NR!=FNR{if(!($0 in a)) print $0}'  php_57 java_940 > java_934
