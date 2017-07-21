data = sqlContext.sql("select distinct id from wl_base.t_base_weibo_user_new where verified='True' and name regexp '信用卡'")
data.saveAsTable("wl_service.t_lel_weibo_credit_tmp")
select group_concat(a.str,"|") from
(select cast(id as string) as str,'1' as sym from wl_service.t_lel_weibo_credit_tmp) a group by a.sym

create table wl_service.t_lel_xinye_extract_wbid
as
select id from t_base_weibo_user_fri where ids regexp '2653476027|1886461330|5120812427|2647910907|5426931847|2650116405|3096171005|2420767405|1749842582|2132440915|2230726611|2024329087|2834639194|1906492355|2271885443|1835498147|1885188752|1909617022|2606171090|1904779373|2180534673|3289781935|1905454404|2282422662|5372300392|5265726792|2073814897|1904835297|2553182725|3016700525|2625170522|2791697975|2581049871|1885780575|1047616300|3305230521|1886176502|2366904410|2133360114|1092901504|5500499926|2279372327|5524113526|2533841282|2107402840|5671129137|2880150542|2422330942|2109054937|2900230837|5463930369|5246589825|3495704624|3283360417|2015961721|1885007321|1906035645|3091475941|5193435512|3231673734|3160778734|2806337734|3268271143|1900893201|2874531644|3828590644|1431249993|5242543888|1923951051|1674412451|3168391547|1880609651|2624208747|2939780103|2497107061|2250353661|1907753765|3974533879|1937021493|2609956060|2979408460|3474382640|1769152985|2397449481|1982231953|3197910312|2954005042|2523235442|2254034540|3500258540|2616190185|2500546245|2006705932|1568520532|2985771430|2116495934|2219516763|5216620082|1892789787|5287149751|1904274833|1819782233|3337885720|3379563072|2530921731|2457425731|2189031670|1764410660|5268986557|2832518802|2959220814|3879276809|1903154525|2116515894|2833725034|1956549780|2753134753|1919118657|1556230122|1919108821|2937514317|3964059517|2677354117|2617822962|1889169663|3187187561|2052879865|2167147054|5623202739|2385870832|5747197561|1934436437|1875086781|3693555277|3697251407|2355263075|5242006674|5073736747|1925068705'
13140300

create table wl_service.t_lel_non_xinye_extract_phone
as
SELECT distinct phone from wl_base.t_base_credit_bank where ds = '20170306' and platform not regexp '兴业' and flag = 'True'
    1234501

create table wl_service.t_lel_xinye_bank_phone_tbid(phone string,tbid string) location '/trans/lel/xinye_bank_tb'
select count(1) from (select distinct phone from wl_service.t_lel_xinye_bank_phone_tbid  where tbid <> 'None')a
	1099337

drop table wl_service.t_lel_xinye_bank_tb_user_active_score;
create table wl_service.t_lel_xinye_bank_tb_user_active_score
as
select a.user_id,b.phone from
(
select m.user_id from
(select c.user_id,row_number() over(partition by c.active_score order by c.active_score desc) rn
from
(select user_id,round((sum(pow(2.8, datediff* (-0.005)))+20)/75,2) as active_score from wl_analysis.t_base_record_cate_simple_ds group by user_id)c)m where m.rn < 42596825
)a
join
(select tbid,phone from wl_service.t_lel_xinye_bank_phone_tbid where tbid <> 'None')b
on a.user_id = b.tbid
    1153473
select count(1) from (select distinct phone from  wl_service.t_lel_xinye_bank_tb_user_active_score)a
     990890

drop table wl_service.t_lel_xinye_bank_consume_scale_gt4_tbids;
create table wl_service.t_lel_xinye_bank_consume_scale_gt4_tbids
as
select a.user_id,a.regtime,b.phone from
( select tmp.uid as user_id,tmp.level as level,tmp.regtime as regtime from
(select uid,regexp_extract(verify,"\\d+",0) as level,regexp_replace(regtime,'\\.','') as regtime from wl_base.t_base_ec_tb_userinfo where verify regexp '\\d+' and ds = '20160608') tmp where cast(tmp.level as int) >3
)a
join
(select tbid,phone from wl_service.t_lel_xinye_bank_phone_tbid where tbid <> 'None')b
on a.user_id = b.tbid
204876
drop table wl_service.t_lel_xinye_bank_consume_feature;
create table wl_service.t_lel_xinye_bank_consume_feature
as
select a.user_id as tbid,a.phone,b.regtime as regtime from
(select user_id,phone from wl_service.t_lel_xinye_bank_tb_user_active_score)a
join
(select user_id,regtime from wl_service.t_lel_xinye_bank_consume_scale_gt4_tbids)b
on a.user_id = b.user_id
200900
select count(1) from (select distinct phone from wl_service.t_lel_xinye_bank_consume_feature where (2017 - cast(substr(regtime,1,4) as int)) > 3 )a
199638


select count(1) from(
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature where (2017 - cast(substr(regtime,1,4) as int)) > 3 )a
join
(select tb_id from t_base_user_profile_telindex where (xianyu_birthday between 22 and 40 ) and xianyu_gender = '1')b
on a.tbid=b.tb_id
)c

select count(1) from(
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature where (2017 - cast(substr(regtime,1,4) as int)) > 3 )a
join
(select tb_id from t_base_user_profile_telindex where (xianyu_birthday between 22 and 40))b
on a.tbid=b.tb_id
)c


select count(1) from(
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature where (2017 - cast(substr(regtime,1,4) as int)) > 3 )a
join
(select tb_id from t_base_user_profile_telindex where (xianyu_birthday between 22 and 40 ) and xianyu_gender = '1')b
on a.tbid=b.tb_id
join
(SELECT user_id FROM t_wrt_consume_tbid_pianhao_monthall where isfood_monthall = '1')c
on a.tbid = c.user_id
)d





create table wl_service.t_lel_xinye_bank_product_type1
as
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature )a
join
(select tb_id from wl_base.t_base_user_profile_telindex where (xianyu_birthday between 22 and 40 ) and xianyu_gender = '1')b
on a.tbid=b.tb_id
join
(SELECT user_id FROM wl_usertag.t_wrt_consume_tbid_pianhao_monthall where isfood_monthall = '1')c
on a.tbid = c.user_id

create table wl_service.t_lel_xinye_bank_product_type2
as
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature  where (2017 - cast(substr(regtime,1,4) as int)) > 3 )a
join
(select tb_id from wl_base.t_base_user_profile_telindex where (xianyu_birthday between 22 and 40 ))b
on a.tbid=b.tb_id
join
(SELECT user_id FROM wl_usertag.t_wrt_consume_tbid_pianhao_monthall where isgame_monthall = '1')c
on a.tbid = c.user_id

create table wl_service.t_lel_xinye_bank_product_type3
as
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature  where (2017 - cast(substr(regtime,1,4) as int)) > 3 )a
join
(select tb_id from wl_base.t_base_user_profile_telindex where (xianyu_birthday between 22 and 40 ))b
on a.tbid=b.tb_id
join
(SELECT user_id FROM wl_usertag.t_wrt_consume_tbid_pianhao_monthall where isfood_monthall = '1' or isfood_monthall = '1')c
on a.tbid = c.user_id

create table wl_service.t_lel_xinye_bank_product_type4
as
select distinct a.phone from
(select tbid,phone from wl_service.t_lel_xinye_bank_consume_feature)a
join
(select tb_id from wl_base.t_base_user_profile_telindex where (xianyu_birthday between 22 and 40 ))b
on a.tbid=b.tb_id
join
(SELECT user_id FROM wl_usertag.t_wrt_consume_tbid_pianhao_monthall where isgame_monthall = '1' or isfood_monthall = '1' and ismuying_monthall = '1')c
on a.tbid = c.user_id


create table wl_service.t_lel_xinye_bank_product_type2_1k
as
select * from wl_service.t_lel_xinye_bank_product_type2 limit 1000


create table wl_service.t_lel_xinye_bank_product_type1_1k
as
select * from wl_service.t_lel_xinye_bank_product_type1 where phone not in (select phone from wl_service.t_lel_xinye_bank_product_type2_1k) limit 1000

create table wl_service.t_lel_xinye_bank_product_type3_1k
as
select a.phone from t_lel_xinye_bank_product_type3 a
where a.phone not in (
select * from t_lel_xinye_bank_product_type2_1k
union all
select * from t_lel_xinye_bank_product_type1_1k
) limit 1000


create table wl_service.t_lel_xinye_bank_product_type4_1k
as
select a.phone from t_lel_xinye_bank_product_type4 a
where a.phone not in (
select * from t_lel_xinye_bank_product_type2_1k
union all
select * from t_lel_xinye_bank_product_type1_1k
union all
select * from t_lel_xinye_bank_product_type3_1k
) limit 1000





