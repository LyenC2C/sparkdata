# coding:utf-8
import re
import sys


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


for line in sys.stdin:
    fields = line.strip().split('\t')
    content = fields[-1]
    if type(content) == type(u""):
        content.encode("utf-8")
    else:
        content
    keywords_1 = '纠纷|坏账|呆账|欠条|刷卡机|瑞银信|破人脸|秒过人脸|法律纠纷|债权债务|陆金所|红岭创投|微贷网|聚宝匯|聚宝汇|宜贷网|爱钱进|宜人贷|团贷网|翼龙贷|你我贷|嘉卡贷|搜易贷|好贷宝|有利网|拿去花|人人贷|温商贷|开鑫贷|博金贷|365易贷|珠宝贷|新新贷|和信贷|连资贷|捷信分期|诚信贷|量化派|微粒贷|拍拍贷|拍拍满|贷款|银行流水|抵押|社保|提现|借呗|京东白条|平安易贷|任性付|易贷网|逾期|不良记录|征信|黑户|解套回款|身份证抠图|无视黑白|无征信|无视征信|黑户贷款|黑户下卡|贷款包装|纯白户面皮|白户贷款|电核流水|征信代打|无视网黑|代做企业对公流水账|代做普通流水账|工资流水|包过电核|企业邮箱|单位邮箱|资金周转|融资|借钱|典当行|放贷|还款|拆借|快借|负债|催收|资金困难|高息|低利率|资金短缺|联贷|银行信贷| 快钱|借条|信贷资金|本息|消费信贷|快易花|亲亲小贷|马上贷|马上金融|51人品贷|瞬时贷|随心贷|信用金|广发零用钱|现金巴士|极速贷|手机贷|租房贷|用钱宝|小秒钱包|58消费贷|闪电借款|宜人极速贷|卡卡贷|卡拉卡|捷信|佰仟|有用分期|玖富|恒昌|小微贷|飞贷|大学分期|民工分期|蓝领分期|好贷|催款函|答辩状|典当|罚单|反诉|房产纠纷|行政处罚|行政复议|行政诉讼|合同纠纷|老赖|老账|欠款|强制执行|取保候审|上诉书|上诉状|诉讼|诉讼法|诉讼状|诉状|损害赔偿|违约|刑事犯罪|债权|失信|被执行|债务|质押|自诉|出险|官司|起诉|起诉书|起诉状|遗赠|遗嘱|再审'
    keywords_2 = '离婚|财产约定|财产协议|套现|t现|未实名制|网银代缴|网银代付|收款|pos|验证码|抢标|闪电认证|信用卡|无担保|不需担保|秒批|秒到|提额|降额|借款|放款|下款|低息|急需资金|拿钱|补交|月息|保单|代交|缺钱|得款|审核|大额|燃眉之急|审批|过桥|打卡|银行利息|闪电|计息|正规渠道|代还|通过率|垫资|面签|变现|银行利率|账号|尾款|免息|销户|黑白|限制|工资款|闲钱|代打|代开|燃眉|转账|小号|大批量|邮箱|法律顾问|法律文书|法律咨询|法务|合同文书|经济合同|劳动合同|律师|律师函|律师事务所|起草|协议书|續費|入股'
    return_1 = list(set(re.findall(str.upper(keywords_1), str.upper(content))))
    flag_1 = "%d" % len(return_1)
    kw_1 = '|'.join(return_1)
    return_2 = list(set(re.findall(str.upper(keywords_2), str.upper(content))))
    flag_2 = "%d" % len(return_2)
    kw_2 = '|'.join(return_2)
    fraud_score = "%d" % (len(return_1) * 2 + len(return_2) * 1)
    keywords = '|'.join(return_1 + return_2)
    fields.extend([flag_1, kw_1, flag_2, kw_2, fraud_score, keywords])
    print "\t".join(fields)
