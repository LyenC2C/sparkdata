本期的欣颜数据需求主要为牙美客群：
a)近三个月购买洁牙,洗牙,黄牙”其中之一关键词产品的客群；
b)地区成都；
c)输出字段：号码 姓名  性别 关键词；
d)提取数量500；


#注意一个用户购买不同关键词的商品

set hive.merge.mapredfiles=true;
create table if not exists wlservice.t_lel_ec_xinyan_20170106_cd_igk_test as 
select t2.id as id,t1.gender as gender,concat_ws(',',collect_set(t2.keyword)) as keyword from 
(select tb_id,xianyu_gender as gender from wlbase_dev.t_base_user_profile_telindex where  tb_location regexp '成都') t1
join
(select user_id as id,regexp_extract(title,'洁牙|洗牙|黄牙|电动牙刷|牙线',0) as keyword
 from wl_base.t_base_ec_record_dev_new 
 where
(cast(dsn as int) 
 between  20161009
 and 20170109)
 and  title regexp '洁牙|洗牙|黄牙|电动牙刷|牙线'
 ) t2
on t1.tb_id = t2.id group by id,gender


cs220:/hive/warehouse/wlservice.db/t_lel_ec_xinyan_20170106_cd_igk
taobao id 换电话
放在这儿:/user/lel/xinyan


1.join data requested

awk -F '\001' 'NR==FNR && $3!="None"{a[$2]=$1"\t"$3}NR!=FNR{if(($1 in a))print a[$1]"\t"$2"\t"$3}' t_lel_ec_xinyan_20170106_cd_igk.teltbname xinyan_20170106_cd_igk , sort , uniq >> 20170106

1.distincted_tel

awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a))print $0}' teeth_wrt_tel 20170106 >> 20170106_distincted

3.hadoop fs -put 20170106_result /user/lel/results
