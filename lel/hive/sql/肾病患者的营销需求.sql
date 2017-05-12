骨病患者淘宝及微博习惯:

ec:
set hive.execution.engine=spark;
set hive.merge.sparkfiles=true;
create table wl_service.t_lel_ec_gubinghuanzhe_about
	as
	select a.user_id from 
(SELECT user_id FROM wl_base.t_base_ec_record_dev_new 
where ds='true' 
and title regexp "麦德威护踝踝关节固定支具支架|Ober护踝脚踝骨折固定支具|麦德威膝关节固定支具|ATAN篮球护踝扭伤防护|雅德老人走路助行器骨折|羽扬老人拐杖伸缩四脚拐棍|ober前臂吊带|ober腰椎术后固定支具支架|脊椎胸椎压缩性骨折支具|麦德威胸腰椎固定支具可调支架矫形器护具|
				 (Swisse|汤臣倍健|钙尔奇).*钙片|(美国Schiff MoveFree|普丽普莱|维骨力).*氨基葡萄糖|(汤臣倍健|GNC).*硒元素|(捷尼康|Schiff MoveFree).*骨胶原|(汤臣倍健|禾博士|childlife|康恩贝).*维生素D|(善存|金施尔康|21金维他).*维生素|(迪巧|恩贝施).*儿童钙")a
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on
a.user_id=b.tb_id



weibo:
1602070971|1337233917|1213494361|1338655765|2877552141|2155700612|234517772|37993288|2877552141|573311928|2621659415|1662475182|1661281175|1661281175|1894287893|1400776392|1159402801|2272764361|1770826840|5913229998|2339206807|3953745253|5468891022


set hive.execution.engine=spark;
set hive.merge.sparkfiles=true;
create table wl_service.t_lel_weibo_gubinghuanzhe_about_2
	as
	select A.id from
(select id from wl_base.t_base_weibo_user_fri where ids regexp '1602070971|1337233917|1213494361|1338655765|2877552141|2155700612|234517772|37993288|2877552141|573311928|2621659415|1662475182|1661281175|1661281175|1894287893|1400776392|1159402801|2272764361|1770826840|5913229998|2339206807|3953745253|5468891022')A
left semi join 
(select id from wl_base.t_base_weibo_user_new where `location` regexp '成都')B
on A.id=B.id






肾病患者的营销需求:

新浪微博:
付平_华西 1933922642
李青大夫  1403418835
ACR陈舟医生 1794814505
中医肖相如  1069266662
肾病医师李建民  2309515393
内科肾内科李嘉华_百歌医学 1919440540
北京联科中医肾病医院莫非凡 3172690077
联科肾病专家刘驰华 3142304435
肾科医生刘乔峰  1648792581
毛志国-肾科医生  2510278004
肾脏内科张冉 5983104188
肾外科大夫   2951947341
肾事坎坷 2312196485
青岛静康肾病医院-吴士云 2312196485
华西医院肾脏科 2141644814
成医附院肾病科 2692697804
京东誉美肾病医院 2013702115
山东潍坊复能肾病医院 2485584101
关注肾病 2147045512
糖尿病食疗专家谢克华 5116569680
健康顾问李景仁 1498651723
许樟荣医生 1927013245
杨历军_糖尿病之敌 1340438733
糖尿病与快乐生活-徐峰 2186480255
邹大进 2023983335
糖尿病研究院院长朱医生 5578063472
祝捷医生  1547578905
上海甲状腺结节糖尿病主任周里钢  2560024420
华西医院糖尿病微创外科治疗中心  2703082040
青城道医胡孝荣  2606939053
糖尿病之友杂志  2178994292
糖尿病教育新7点  2254645210
爱问医生-糖尿病  5912831472
糖尿病网  1857108075
成都瑞恩糖尿病医院 2523720952


set hive.execution.engine=spark;
set hive.merge.sparkfiles=true
create talbe wl_service.t_lel_shen_about
	as
select A.user_id from wl_base.t_base_weibo_fri A where ids regexp '1933922642|1403418835|1794814505|1069266662|2309515393|1919440540|
															   3172690077|3142304435|1648792581|2510278004|5983104188|2951947341|
															   2312196485|2141644814|2692697804|2013702115|2485584101|2147045512|
															   5116569680|1498651723|1927013245|1340438733|2186480255|2023983335|
															   5578063472|1547578905|2560024420|2703082040|2606939053|2178994292|
															   2254645210|5912831472|1857108075|2523720952'
left semi join 
select B.user_id from wl_base.t_base_weibo_new B where location regexp '成都'
on															   
A.user_id=B.user_id

消费行为

create table wl_service.t_lel_shen_consume_about
	as
	select a.user_id from 
(SELECT user_id FROM wl_base.t_base_ec_record_dev_new 
where ds='true' 
and title regexp "糖尿病人餐谱一本就够了|救命之方|糖尿病治疗与保养大全|糖尿病饮食宜忌指南|向红丁-糖尿病饮食宜忌+糖尿病饮食运动|养肾就是养命|肾病怎么吃|
	                        慢性肾病防治与调养全书|肾好才是硬道理|养生先养肾|肾病营养配餐1288例|肾病生活调养防治800问|三高这样吃降得快|胡大一高血压高血脂饮食＋运动|
	                        三高调养食谱一本全|匀浆膳|低蛋白大米|逸盛麦淀粉低蛋白面条|逸盛低蛋白特制麦淀粉|立适康均衡全营养粉|立适康乳清蛋白粉|透析压脉带|
	                        肾炎四味片|肾复康胶囊|肾炎康复片|肾衰宁|金水宝胶囊|百令胶囊|心肝宝|肾肝宁|黄葵胶囊|血尿胶囊|肾炎片|肾炎舒胶囊|海昆肾喜|尿毒清颗粒|
	                        爱西特|析清（包醛氧淀粉）|开同（复方α-酮酸片|保肾康（阿魏酸哌嗪片）|金水宝|百令胶囊|朗仕羟苯磺酸钙|怡开胰激肽原酶|血糖仪|血糖试纸|胰岛素注射笔针头|
	                        血压计|鱼油软胶囊|中膳堂无蔗糖粗粮燕麦饼干|香楠无蔗糖杂粗粮饼干|百合苦瓜洋参软胶囊|宫诺葛根苦瓜铬胶囊|沂山康宝蜂胶软胶囊")a
left semi join 
(select tb_id from wl_base.t_base_user_profile_telindex where tel_loc regexp '成都')b
on
a.user_id=b.tb_id



set hive.execution.engine=spark;
set hive.merge.sparkfiles=true;	
create table wl_service.t_lel_shen_about
	as
select A.id from 
(select id from wl_base.t_base_weibo_user_fri  where ids regexp '1933922642|1403418835|1794814505|1069266662|2309515393|1919440540|3172690077|3142304435|1648792581|2510278004|5983104188|2951947341|2312196485|2141644814|2692697804|2013702115|2485584101|2147045512|5116569680|1498651723|1927013245|1340438733|2186480255|2023983335|5578063472|1547578905|2560024420|2703082040|2606939053|2178994292|5578063472|1547578905|2560024420|2703082040|2606939053|2178994292|2254645210|5912831472|1857108075|2523720952')A
left semi join 
(select id from wl_base.t_base_weibo_user_new where location regexp '成都')B
on															   
A.id=B.id




awk -F '\t' 'NR==FNR{a[$1]=$2}NR!=FNR{if( $1 in a ) print $1"\t"a[$1]}' liuxue_multi.txt liuxue_multi_uniq.txt | wc -l





product:

awk -F '\001' '{if($1!="None")print $1}' t_lel_shen_weibo_about.teltb | sort -R | uniq| head -2000  > shen_weibo_2000
awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a) && $1!="None") print $1}' shen_weibo_2000 t_lel_shen_consume_about.teltb | sort -R | uniq | head -8000 > shen_ec


20170414_marketing_needs:




20170418:
create table wl_service.t_lel_shen_4ids_medicalandbaojian_20170418
as
select user_id,brand_name,dsn,concat("https://item.taobao.com/item.htm?id=",cast(item_id as string)) as url from wl_base.t_base_ec_record_dev_new where ds='true' and user_id regexp '446310672|492623249|318183829|2323342776' and cast(dsn as int ) > 20160418
and root_cat_id in ("50020274",'50026800','50008825','50023717','124164003')

select b.phone,a.brand_name,a.dsn,a.url
from
(select * from wl_service.t_lel_shen_4ids_medicalandbaojian_20170418)a
join 
(select  phone,tbid from wl_service.t_lel_shen_20170414_marketing_needs)b
on a.user_id = b.tbid
