1769152985 招商银行信用卡
1697486122 拉卡拉

set hive.merge.mapredfiles=True;
set hive.merge.mapfiles=True;
create table if not exists wl_service.t_lel_weibo_yixin_20170209
as
select 
t1.id as id,
case when ids = '1769152985' then '招商银行信用卡' when ids = '1697486122' then '拉卡拉' end as weibo_name
from 
(select id,regexp_extract(ids,'1769152985|1697486122',0) as ids
from
wl_base.t_base_weibo_user_fri
where
ds='20161106' and ids regexp '1769152985|1697486122')t1
join
(select id 
from
wl_base.t_base_weibo_user_new
where ds='20161123' and verified='True')t2
on t1.id=t2.id


hadoop fs -cat /user/lel/results/yixin/lakala20170213/* > lakala
hadoop fs -cat /user/lel/results/yixin/except_lakala20170213/* > except_lakala

bank:

awk -F '\t' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a ))print $0}' platform_no_regist res_bank >res_bank_platform_no_regist
awk -F '\t' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a ))print $0}' ../weibo_non-app/previous_platform_weibo  res_bank_platform_no_regist > res_bank_platform_no_regist_weibo_pre
head -15000 res_bank_platform_no_regist_weibo_pre > res_bank_platform_no_regist_weibo_pre_1.5w

weibo:
hadoop fs -cat /user/lel/results/yixin/previous/* > previous_platform
awk -F '\t' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a ))print $0}' previous_platform res_19693 > weibo_credit







