--1)合并淘宝商品数据(淘宝商品基础表+采集的推荐商品表+异常关键词采集的商品)
insert OVERWRITE TABLE wl_service.t_lt_base_item_all_week
SELECT item_id,cat_id,cat_name,root_cat_name,seller_id,title from
(SELECT item_id,cat_id,cat_name,root_cat_name,seller_id,title from wl_service.t_lt_crawler_recommend_item_info
union all
SELECT item_id,cat_id,cat_name,root_cat_name,seller_id,title from wl_base.t_base_ec_item_dev_new
where ds='20170506'
union ALL
select item_id,cat_id,cat_name,root_cat_name,seller_id,title from
wl_base.t_base_ec_item_abnormal where ds='20170503')t
GROUP by item_id,cat_id,cat_name,root_cat_name,seller_id,title;


''''

'分类筛选异常商品关键词
1. 套现贷款类(套现设备,网贷口子,网贷技术培训)
"联银鑫宝|宜信普惠|天天付|瑞银信|陆金所|红岭创投|微贷网|聚宝匯|聚宝汇|宜贷网|爱钱进|宜人贷|团贷网|翼龙贷|你我贷|嘉卡贷|搜易贷|好贷宝|有利网|拿去花|人人贷|温商贷|开鑫贷|博金贷|365易贷|珠宝贷|新新贷|和信贷|连资贷|诚信贷|微粒贷|拍拍贷|拍拍满|借趣花|51人品|平安易贷|任性付|51信用卡|易贷网|平安普惠|快易花|亲亲小贷|马上贷|51人品贷|瞬时贷|随心贷|广发零用钱|现金巴士|极速贷|手机贷|租房贷|用钱宝|小秒钱包|58消费贷|闪电借款|宜人极速贷|卡卡贷|卡拉卡|有用分期|小微贷|飞贷|今借到|大学分期|民工分期|蓝领分期|好贷|钱宝网|汇付天下|叮当贷|借贷宝|诺诺镑客|卡利宝|甜橙白条|分期乐|来分期|京东白条|京东白条抢券工具|无视风控|白条代下单|贷款|黑户贷款|小额贷款操作|下款技术|小额贷款|信用贷款|小额借钱|贷款秒下|无视黑白|无征信|无视征信|黑户下卡|白户贷款|无视网黑|满标|等额本息|网贷口子|贷款技术|贷款口子|网贷|一清机|撸羊毛|包下款|薅羊毛|手机POSS|手机POS|移动POS|蓝牙pos|手机刷卡机|手机刷卡器|信用卡刷卡器|强开贷款|信用卡贷款|借款培训|京东套现|抵押套现|还款技术|技术口子|网赚教程|下款利器|玩转信用卡|口子教程|网络速贷|小贷资料|银行口子|极速放款|低息借款|低息代款|金融口子|网贷技术|小贷口子|口子贷|贷口子|手机套现|口子|低息贷款|低息|下款|借款|放米|强开提额|套现"

"金融一体机|质押|典当|乐富|低息|速贷|信用卡|网赚|包装小贷|包装公司|抵押|提现|借呗|黑户|操作|技术|跳码|燃眉之急|信用贷|借钱|借贷|额度|借款|芝麻信用|急用钱|急借钱|典当行|放贷|信用卡还款|拆借|快借|资金困难|资金短缺|放米|快钱|套现|变现|t现|急缺钱|口子|急需资金|抢标|秒标|不需担保|无担保|秒批|秒到|提额|降额|放款|下款|缺钱转|点刷|卡乐付|pos机|刷卡机|手刷|一清|多商户|实时到账|费率|乐刷|易付宝|poss机|ps机|蓝牙POS|蓝牙刷卡机|蓝牙刷卡器|POS刷卡器|刷卡器费率|手机POSS|收款宝|拉卡拉|刷卡机|变现|P2P|蓝牙|pos刷卡器|速刷|汇付|秒到账|手机pos|实时到帐|贷款|车贷|机子|手机刷卡器|玖富"

2. 资料造假类(银行流水,企业邮箱账号,社保公积金,学历,公司坏账,工资流水)\黑中介类(电话代接,公司贷款包装)
"银行对账单|提额认证|闪银认证|闪银提额|不良记录|身份证抠图|代做企业对公流水账|代做普通流水账|银行贷款报表|处理逾期|经营异常|借款逾期|账单技术|代过人脸识别|代过各种人脸识别|闪银|贷款|借款|贷款包装|包过电核|固话代接|代接回访|代接固话|信用卡|银行回访|单位包装|贷款回访|电话回访|回访专用|单位回访|公积金转移|电话代接|回访|呼叫转移|银行小贷|下卡|代接|cpa注册"

"贷款|征信代打|电核流水|销户|代做|银行流水|工资流水|收入证明|社保|公积金|挂靠|代缴|代交|邮箱批发|企业邮箱|邮箱|单位邮箱|公司邮箱|邮箱账号|闪银|Wecash闪银|闪电|51人品|资金周转|流水回单"

3.彩票兼职类
"宝妈|赚钱|挣钱|暴利|博彩|彩票|时时彩|七乐彩|双色球|彩票中奖|大乐透|彩票软件|彩票分析软件|500万|中奖符|赌博符|还钱符|网赚|兼职|网赚项目|快速赚钱|偏门|工资日结|营销软件|贷款|网赚教程|京东号|JD号|京东|账号|三绑|全绑|JD|帐号|京东小号|小号批发|三邦|平安|闪银"

4.法律咨询服务类
"借条|催款函|答辩状|罚单|反诉|房产纠纷|行政复议|行政诉讼|合同纠纷|取保|候审|缓刑|强制执行|查封|冻结扣款|失信|老赖|上诉书|上诉状|诉讼状|诉状|律师函|法律咨询|债权债务|借款|贷款|纠纷|抵押担保|质押|打官司|起诉|起诉书|起诉状|遗赠|遗嘱|高利贷"

5.正常词汇筛选
"卡拉卡拉|打印纸|充值|热敏纸|收银|logo|海报|走势图|工作服|T恤|收藏册|书籍|书|摇奖机|箱|纸牌|摇号机|购彩金|元|充值|卡包|卡套|卡夹|小票纸|PPT|考勤|单据|设计|名片|网站建设|视频|留学|翻译|现货"

'''

--2.fraud_filter.py 筛选匹配关键词商品

--3.结果入库
CREATE TABLE  if not exists wl_service.t_lt_base_tb_item_filter_words_all (
item_id STRING  COMMENT  '商品id',
title  STRING   COMMENT '商品title',
cat_id  STRING   COMMENT '商品所属类目id',
cat_name STRING  COMMENT '商品所属类目名称',
root_cat_name STRING  COMMENT '商品顶级类目名称',
seller_id STRING  COMMENT '店家id',
title STRING  COMMENT '商品名',
fscore_1 BIGINT COMMENT '套现贷款',
fscore_11 BIGINT COMMENT '',
fscore_2 BIGINT COMMENT '造假中介',
fscore_22 BIGINT COMMENT '',
fscore_3 BIGINT COMMENT '彩票兼职',
fscore_4 BIGINT COMMENT '法律咨询',
keywords STRING COMMENT 'fraud_words',
keywords_d STRING COMMENT 'normal_words'
)
COMMENT '电商异常商品表'
PARTITIONED by (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;

load data INPATH '/user/lt/taobao/0511/part*' into TABLE wl_service.t_lt_base_tb_item_filter_words_all PARTITION (ds='20170511');


--=======规则筛选
-- 4.class 1
CREATE table t_lt_base_tb_item_fraud_txdk_1 as
select * from t_lt_base_tb_item_filter_words_all
where (fscore_1>1 or fscore_11>2)
and cat_name not in ('数码收纳整理包','收银纸','电源/适配器')
and root_cat_name not in ('书籍/杂志/报纸' ,'电子词典/电纸书/文化用品')
and length(regexp_replace(title,'书籍|LOGO|logo|海报|热敏纸|打印纸|小票纸|PPT|收银纸|卡包|卡套|T恤|工作服|卡夹|考勤',''))=length(title)
and keywords<>'玖富|口子'
union all
select * from t_lt_base_tb_item_filter_words_all
where (fscore_1>1 or fscore_11>2) and title like '%玩转信用卡%' and title like '%玩的就是信用卡%'
union all
select * from t_lt_base_tb_item_filter_words_all
where (fscore_1=1 and fscore_11=2)
and cat_name not in ('数码收纳整理包','收银纸','电源/适配器')
and root_cat_name not in ('书籍/杂志/报纸' ,'电子词典/电纸书/文化用品','古董/邮币/字画/收藏')
and length(regexp_replace(title,'书籍|LOGO|logo|海报|热敏纸|打印纸|小票纸|PPT|收银纸|卡包|卡套|T恤|工作服|卡夹|考勤',''))=length(title)
and keywords<>'玖富|口子';


--5.class 2
CREATE table t_lt_base_tb_item_fraud_zjzj_2 as
SELECT * from t_lt_base_tb_item_filter_words_all
where (fscore_2>1 or fscore_22>2)
and length(regexp_replace(title,'书籍|LOGO|logo|海报|热敏纸|打印纸|小票纸|PPT|收银纸|卡包|卡套|T恤|工作服|卡夹|考勤',''))=length(title);


--6.class 3
CREATE table t_lt_base_tb_item_fraud_cpjz_3 as
SELECT * from t_lt_base_tb_item_filter_words_all
where fscore_3>2 and root_cat_name not in ('运动/瑜伽/健身/球迷用品','书籍/杂志/报纸','玩具/童车/益智/积木/模型','自用闲置转让','五金/工具','电子词典/电纸书/文化用品','商业/办公家具')
and length(regexp_replace(title,'LOGO|logo|海报|走势图|工作服|T恤|收藏册|书|摇奖机|箱|纸牌|摇号机|购彩金|元|充值',''))=length(title);


--7.class 4
CREATE table t_lt_base_tb_item_fraud_flzx_4 as
SELECT * from t_lt_base_tb_item_filter_words_all
where fscore_4>2 AND root_cat_name not in ('书籍/杂志/报纸','教育培训')
and length(regexp_replace(title,'单据|设计|LOGO|logo|名片|网站建设|视频',''))=length(title);

--8.merge all fraud item
create table wl_service.t_lt_base_tb_item_fraud_all as
select item_id,title,cat_id,cat_name,root_cat_name,seller_id,
max(fscore_1+fscore_11+fscore_2+fscore_22+fscore_3+fscore_4) as fraud_score,
collect_set(keywords)[0] as keywords
from
(select * from t_lt_base_tb_item_fraud_txdk_1
union all
select * from t_lt_base_tb_item_fraud_zjzj_2
union all
select * from t_lt_base_tb_item_fraud_cpjz_3
union all
select * from t_lt_base_tb_item_fraud_flzx_4)t
group by item_id,title,cat_id,cat_name,root_cat_name,seller_id;


--=====筛选异常商品评论数据=====================
--9.filter fraud record [union新采集的异常商品评论]

insert into table wl_analysis.t_lt_taobao_fraud_record_week partition(ds='20170504')
SELECT feed_id,cat_id,root_cat_id,shop_id,user_id,bc_type,dsn,item_id,
collect_set(title)[0] as title,max(fraud_score) as fraud_score,collect_set(keywords)[0] as keywords
from
(select t1.feed_id,t1.cat_id,t1.root_cat_id,t1.shop_id,t1.user_id,t1.bc_type,t1.dsn,
t1.item_id,t1.title,t2.fraud_score,t2.keywords from
(SELECT * from wl_service.t_lt_base_sp_recom_item_fraud_all)t2
join
(SELECT item_id,feed_id,title,user_id,bc_type,cat_id,root_cat_id, shop_id, dsn
	from wl_base.t_base_ec_record_dev_new where ds='true')t1
on t1.item_id=t2.item_id
union ALL
select t4.feed_id,t4.cat_id,t4.root_cat_id,t4.shop_id,t4.user_id,t4.bc_type,t4.dsn,
t4.item_id,t4.title,t3.fraud_score,t3.keywords from
(SELECT * from wl_service.t_lt_base_sp_recom_item_fraud_all)t3
join
(SELECT item_id,feed_id,title,user_id,bc_type,cat_id,root_cat_id, shop_id, dsn
	from wl_base.t_base_ec_record_dev_new_inc where ds='20170424')t4
on t3.item_id=t4.item_id)t
group by item_id,feed_id,title,user_id,bc_type,cat_id,root_cat_id, shop_id, dsn,item_id;


--10.check the filter record
SELECT item_id,count(1) as num,collect_set(title)[0] as title,
max(fraud_score) as fraud_score,collect_set(keywords)[0] as keywords
FROM wl_analysis.t_lt_taobao_fraud_record_week
where ds='20170504'
GROUP BY item_id;


--11.statistic fraud item_num,user_num,record_num
select count(1) as item_num from
(select item_id from wl_analysis.t_lt_taobao_fraud_record_week
where ds='20170504' group by item_id)t;

select count(1) as user_num from
(select user_id from wl_analysis.t_lt_taobao_fraud_record_week
where ds='20170504' group by user_id)t;

select count(1) as record_num from
(select feed_id from wl_analysis.t_lt_taobao_fraud_record_week
where ds='20170504' )t;



--==============爬虫周更任务数据提取==================================
create table if not exists wl_analysis.t_lt_crawler_fraud_taobao_item_week(
item_id string comment '商品id',
cat_id string comment '子类目id',
seller_id string comment '卖家id'
)
comment '爬虫采集推荐商品数据表'
partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' stored as textfile;

--1.异常商品评论与推荐商品采集
insert into table wl_analysis.t_lt_crawler_fraud_taobao_item_week partition(ds='20170504')
SELECT item_id,cat_id,seller_id from wl_service.t_lt_base_sp_recom_item_fraud_all


compute stats t_lt_crawler_fraud_taobao_item_week;
show table stats t_lt_crawler_fraud_taobao_item_week;





--=========异常商品买家分析=========
--analysis seller_id
create table wl_service.t_lt_base_fraud_seller_info as
select t1.*,t2.item_count,(t2.item_count/t1.fraud_item_num) as fraud_ratio from
(select seller_id,count(1) as fraud_item_num from
wl_service.t_lt_base_sp_item_fraud_all group by seller_id)t1
left join
(select seller_id,item_count from wl_base.t_base_ec_shop_dev_new
where ds='20170424')t2
on t1.seller_id=t2.seller_id;

CREATE table wl_service.t_lt_base_fraud_seller_item as
SELECT t2.* from
(SELECT seller_id from wl_service.t_lt_base_fraud_seller_info
	where fraud_ratio<11)t1
join
(select * from wl_base.t_base_ec_item_dev_new
where ds='20170424')t2
on t1.seller_id=t2.seller_id


--analysis weibo and taobao fraud user
create table t_lt_fraud_weibo_2_seller_info as
SELECT t1.snwb,t2.name,t2.description,t1.seller_id,t3.shop_id,t3.shop_name,t3.seller_name from
(SELECT snwb,seller_id FROM wl_service.t_lt_fraud_weibo_2_seller_id
where snwb is not null and seller_id is not null group by snwb,seller_id)t1
left join
(SELECT id,name,description from wl_analysis.t_lt_weibo_fraud_usr)t2
on t1.snwb=t2.id
left join
(SELECT seller_id,shop_id,shop_name,seller_name from wl_base.t_base_ec_shop_dev_new where ds='20170424')t3
on t1.seller_id=t3.seller_id


--num_shop
create table wl_service.t_lt_base_ec_seller_shop_count as
select seller_id,count(1) as num_shop from wl_base.t_base_ec_shop_dev_new
where ds='20170415'
group by seller_id having num_shop>1












