#coding:utf-8
# author:lt

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
from pyspark import SparkContext
from pyspark.sql import *

sc=SparkContext(appName='filterItem')
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

keywords_1 =sc.broadcast("联银鑫宝|宜信普惠|天天付|瑞银信|陆金所|红岭创投|微贷网|聚宝匯|聚宝汇|宜贷网|爱钱进|宜人贷|团贷网|翼龙贷|你我贷|嘉卡贷|搜易贷|好贷宝|有利网|拿去花|人人贷|温商贷|开鑫贷|博金贷|365易贷|珠宝贷|新新贷|和信贷|连资贷|诚信贷|微粒贷|拍拍贷|拍拍满|借趣花|51人品|平安易贷|任性付|51信用卡|易贷网|平安普惠|快易花|亲亲小贷|马上贷|51人品贷|瞬时贷|随心贷|广发零用钱|现金巴士|极速贷|手机贷|租房贷|用钱宝|小秒钱包|58消费贷|闪电借款|宜人极速贷|卡卡贷|卡拉卡|有用分期|小微贷|飞贷|今借到|大学分期|民工分期|蓝领分期|好贷|钱宝网|汇付天下|叮当贷|借贷宝|诺诺镑客|卡利宝|甜橙白条|分期乐|来分期|京东白条|京东白条抢券工具|无视风控|白条代下单|贷款|黑户贷款|小额贷款操作|下款技术|小额贷款|信用贷款|小额借钱|贷款秒下|无视黑白|无征信|无视征信|黑户下卡|白户贷款|无视网黑|满标|等额本息|网贷口子|贷款技术|贷款口子|网贷|一清机|撸羊毛|包下款|薅羊毛|手机POSS|手机POS|移动POS|蓝牙pos|手机刷卡机|手机刷卡器|信用卡刷卡器|强开贷款|信用卡贷款|借款培训|京东套现|抵押套现|还款技术|技术口子|网赚教程|下款利器|玩转信用卡|口子教程|网络速贷|小贷资料|银行口子|极速放款|低息借款|低息代款|金融口子|网贷技术|小贷口子|口子贷|贷口子|手机套现|口子|低息贷款|低息|下款|借款|放米|强开提额|套现").value
keywords_11 = sc.broadcast("金融一体机|质押|典当|乐富|低息|速贷|信用卡|网赚|包装小贷|包装公司|抵押|提现|借呗|黑户|操作|技术|跳码|燃眉之急|信用贷|借钱|借贷|额度|借款|芝麻信用|急用钱|急借钱|典当行|放贷|信用卡还款|拆借|快借|资金困难|资金短缺|放米|快钱|套现|变现|t现|急缺钱|口子|急需资金|抢标|秒标|不需担保|无担保|秒批|秒到|提额|降额|放款|下款|缺钱转|点刷|卡乐付|pos机|刷卡机|手刷|一清|多商户|实时到账|费率|乐刷|易付宝|poss机|ps机|蓝牙POS|蓝牙刷卡机|蓝牙刷卡器|POS刷卡器|刷卡器费率|手机POSS|收款宝|拉卡拉|刷卡机|变现|P2P|蓝牙|pos刷卡器|速刷|汇付|秒到账|手机pos|实时到帐|贷款|车贷|机子|手机刷卡器|玖富").value
keywords_2 = sc.broadcast("银行对账单|提额认证|闪银认证|闪银提额|不良记录|身份证抠图|代做企业对公流水账|代做普通流水账|银行贷款报表|处理逾期|经营异常|借款逾期|账单技术|代过人脸识别|代过各种人脸识别|闪银|贷款|借款|贷款包装|包过电核|固话代接|代接回访|代接固话|信用卡|银行回访|单位包装|贷款回访|电话回访|回访专用|单位回访|公积金转移|电话代接|回访|呼叫转移|银行小贷|下卡|代接|cpa注册").value
keywords_22 = sc.broadcast("贷款|征信代打|电核流水|销户|代做|银行流水|工资流水|收入证明|社保|公积金|挂靠|代缴|代交|邮箱批发|企业邮箱|邮箱|单位邮箱|公司邮箱|邮箱账号|闪银|Wecash闪银|闪电|51人品|资金周转|流水回单").value
keywords_3 = sc.broadcast("宝妈|赚钱|挣钱|暴利|博彩|彩票|时时彩|七乐彩|双色球|彩票中奖|大乐透|彩票软件|彩票分析软件|500万|中奖符|赌博符|还钱符|网赚|兼职|网赚项目|快速赚钱|偏门|工资日结|营销软件|贷款|网赚教程|京东号|JD号|京东|账号|三绑|全绑|JD|帐号|京东小号|小号批发|三邦|平安|闪银").value
keywords_4 = sc.broadcast("借条|催款函|答辩状|罚单|反诉|房产纠纷|行政复议|行政诉讼|合同纠纷|取保|候审|缓刑|强制执行|查封|冻结扣款|失信|老赖|上诉书|上诉状|诉讼状|诉状|律师函|法律咨询|债权债务|借款|贷款|纠纷|抵押担保|质押|打官司|起诉|起诉书|起诉状|遗赠|遗嘱|高利贷").value
delete_w = sc.broadcast("卡拉卡拉|打印纸|充值|热敏纸|收银|logo|海报|走势图|工作服|T恤|收藏册|书籍|书|摇奖机|箱|纸牌|摇号机|购彩金|元|充值|卡包|卡套|卡夹|小票纸|PPT|考勤|单据|设计|名片|网站建设|视频|留学|翻译|现货").value



def parse(line):
    title = line[-1]
    if type(title) == type(u""):
        title = title.encode("utf-8").replace(" ", "")
    return_1 = list(set(re.findall(str.upper(keywords_1), str.upper(title))))
    fscore_1 = "%d" % len(return_1)
    return_11 = list(set(re.findall(str.upper(keywords_11), str.upper(title))))
    fscore_11 = "%d" % len(return_11)
    return_2 = list(set(re.findall(str.upper(keywords_2), str.upper(title))))
    fscore_2 = "%d" % len(return_2)
    return_22 = list(set(re.findall(str.upper(keywords_22), str.upper(title))))
    fscore_22 = "%d" % len(return_22)
    return_3 = list(set(re.findall(str.upper(keywords_3), str.upper(title))))
    fscore_3 = "%d" % len(return_3)
    return_4 = list(set(re.findall(str.upper(keywords_4), str.upper(title))))
    fscore_4 = "%d" % len(return_4)
    keywords_d = '|'.join(list(set(re.findall(str.upper(delete_w), str.upper(title)))))
    keywords = '|'.join(set(return_1 + return_11 + return_2 + return_22 + return_3 + return_4))
    #filter
    if fscore_1!='0' or fscore_11!='0' or fscore_2!='0' or fscore_22!='0' or fscore_3!='0' or fscore_4!='0':
        items ="\001".join(["%s" %i for i in line]).encode("utf-8")
        return_list = "\001".join([fscore_1, fscore_11, fscore_2, fscore_22, fscore_3, fscore_4, keywords,keywords_d])
        items = items+"\001"+return_list
    else:items="null"
    return items

#筛选淘宝商品
sql_item='''
select * from wl_service.t_lt_base_item_all_week
'''

rdd_table = hiveContext.sql(sql_item)
rdd_table =rdd_table.na.fill("\\N")
rdd_new=rdd_table.map(lambda x:parse(x))
return_rdd = rdd_new.filter(lambda x:x!="null")
return_rdd.saveAsTextFile("/user/lt/taobao/0511")


#test
sql_item='''
select * from wl_service.t_lt_base_item_all_week limit 10000
'''


#入库
# CREATE TABLE  if not exists wl_service.t_lt_base_tb_item_filter_words_all (
# item_id STRING  COMMENT  '商品id',
# title  STRING   COMMENT '商品title',
# cat_id  STRING   COMMENT '商品所属类目id',
# cat_name STRING  COMMENT '商品所属类目名称',
# root_cat_name STRING  COMMENT '商品顶级类目名称',
# seller_id STRING  COMMENT '店家id',
# title STRING  COMMENT '商品名',
# fscore_1 BIGINT COMMENT '套现贷款',
# fscore_11 BIGINT COMMENT '',
# fscore_2 BIGINT COMMENT '造假中介',
# fscore_22 BIGINT COMMENT '',
# fscore_3 BIGINT COMMENT '彩票兼职',
# fscore_4 BIGINT COMMENT '法律咨询',
# keywords STRING COMMENT 'fraud_words',
# keywords_d STRING COMMENT 'normal_words'
# )
# COMMENT '电商异常商品表'
# PARTITIONED by (ds STRING)
# ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'  LINES TERMINATED BY '\n' stored as textfile;
#
# load data INPATH '/user/lt/FilterKeywords/0505/part*' into TABLE wl_service.t_lt_base_taobao_item_filtered_all PARTITION (ds='20170505');