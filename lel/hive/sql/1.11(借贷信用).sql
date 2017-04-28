01.11(借贷信用).sql

客户的借贷信用保险产品目标客群定位需求如下:

1,近三个月购买汽车周边品类商品用户  数量3000

2,微博关注http://weibo.com/cpic的粉丝，数量3000   1730276312

输出字段号码，区分来源即可。
以上需求数据结果在周五之前输出即可

1.
set hive.merge.mapfiles = true;
create table if not exists wlservice.t_lel_ec_qiche_20170111
as
select user_id from 
wl_base.t_base_ec_record_dev_new 
where
ds='true'
and
(cast(dsn as int) between 20161011 and 20170111)
and 
root_cat_id in ('50016768','26')


cs220:/hive/warehouse/wlservice.db/t_lel_ec_qiche_20170111
taobao id 换电话
放在这儿:/user/lel/jiedai   




awk 'BEGIN{FS="\n";RS=""}{i_cont=3000;rtol=1243732;x=1;for(i=0;i {i_r=rand();x += int((rtol-x)/(i_cont-i)*i_r);print i_r"\t"x"\t"$x}}' qiche_20170111_number_tbid_name > ggg.txt[@more@] 
	


2.
set hive.merge.mapredfiles = true;
create table if not exists wl_service.t_lel_weibo_taipingyang_20170111 
as
select t1.id as id 
from
(select
id
from t_base_weibo_user_fri
where ds = 20161106
and ids regexp '1730276312'
) t1
join
(select
id,followers_count
from t_base_weibo_user_new
where ds = '20161123'
order by followers_count desc
) t2
on t1.id = t2.id


cs105:/hive/warehouse/wl_service.db/t_lel_weibo_taipingyang_20170111
微博id换电话
放在cs105:/user/lel/weibo
awk -F '\001' 'NR=FNR{a[$1]=""}{if(($2 in a && $3 !="Nt1"))print $1"\t"$3}' taipingyang taipingyang.telsnwbname > taipingyang_2169