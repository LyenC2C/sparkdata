xinyan20170209

欣颜牙美继续与之前需求一致：
a)近三个月有购买过洁牙、洗牙、儿童牙膏、电动牙刷、牙线、漱口水的关键词商品用户；
b)成都地区；
c)数量500；
d)输出字段号码、姓名、性别、关键词。


set hive.merge.mapredfiles=true;
create table if not exists wlservice.t_lel_ec_xinyan_20170209_cd_igk as 
select t2.id as id,t1.gender as gender,concat_ws(',',collect_set(t2.keyword)) as keyword from 
(select tb_id,xianyu_gender as gender from wlbase_dev.t_base_user_profile_telindex where  tb_location regexp '成都') t1
join
(select user_id as id,regexp_extract(title,'洁牙|洗牙|儿童牙膏|电动牙刷|牙线|漱口水',0) as keyword
 from wl_base.t_base_ec_record_dev_new 
 where ds='true'
 and
(cast(dsn as int) 
 between  20170209
 and 20161209)
 and  title regexp '洁牙|洗牙|儿童牙膏|电动牙刷|牙线|漱口水'
 ) t2
on t1.tb_id = t2.id group by id,gender

1.join data requested

awk -F '\001' 'NR==FNR && $3!="None"{a[$2]=$1"\t"$3}NR!=FNR{if(($1 in a))print a[$1]"\t"$2"\t"$3}' t_lel_ec_xinyan_20170112_cd_igk.teltbname xinyan_20170209_cd_igk | sort | uniq >> 20170209

1.distincted_tel

awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a))print $0}' teeth_wrt_tel 20170209 >> 20170209_distincted

3.hadoop fs -put 20170209_distincted_500 /user/lel/results