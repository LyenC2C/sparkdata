#coding:utf-8
__author__ = 'zlj'

import re
import sys
def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res
    else: return res

for line in sys.stdin:
    fields = line.strip().split('\t')
    title=fields[-1]
    if type(title) == type(u""):
        title.encode("utf-8")
    else:
        title
    title=title+''
    keywords_1 = '花呗|借呗|纠纷|坏账|呆账|欠条|刷卡机|瑞银信|破人脸|秒过人脸|法律纠纷|债权债务|陆金所|红岭创投|微贷网|聚宝匯|聚宝汇|宜贷网|爱钱进|宜人贷|团贷网|翼龙贷|你我贷|嘉卡贷|搜易贷|好贷宝|有利网|拿去花|人人贷|温商贷|开鑫贷|博金贷|365易贷|珠宝贷|新新贷|和信贷|连资贷|捷信分期|诚信贷|量化派|微粒贷|拍拍贷|拍拍满|贷款|套现|银行流水|抵押|社保|提现|借呗|京东白条|平安易贷|任性付|易贷网|逾期|不良记录|征信|黑户|解套回款|身份证抠图|无视黑白|无征信|无视征信|黑户贷款|黑户下卡|贷款包装|纯白户面皮|白户贷款|电核流水|征信代打|无视网黑|代做企业对公流水账|代做普通流水账|工资流水|包过电核|企业邮箱|单位邮箱|t现|T现|资金周转|融资|借钱|典当行|放贷|还款|拆借|快借|负债|催收|资金困难|高息|折现|低利率|资金短缺|联贷|银行信贷|快钱|借条|信贷资金|本息|消费信贷'
    keywords_2 = '未实名制|网银代缴|网银代付|收款|vspos|VSPOS|验证码|抢标|闪电认证|信用卡|无担保|不需担保|秒批|秒到|提额|降额|借款|放款|下款|低息|急需资金|拿钱|补交|月息|保单|代交|缺钱|得款|审核|大额|燃眉之急|审批|过桥|打卡|银行利息|闪电|计息|正规渠道|代还|通过率|垫资|面签|变现|银行利率|账号|尾款|免息|销户|黑白|限制|工资款|闲钱|代打|代开|燃眉|转账|小号|大批量|邮箱'
    return_1 = list(set(re.findall(keywords_1, title)))
    flag_1 = "%d"%len(return_1)
    kw_1 = '|'.join(return_1)
    return_2 = list(set(re.findall(keywords_2, title)))
    flag_2 = "%d"%len(return_2)
    kw_2 = '|'.join(return_2)
    fraud_score = "%d"%(len(return_1)*2 + len(return_2)*1)
    keywords = '|'.join(return_1+return_2)
    fields.extend([fraud_score,keywords])
    print "\t".join(fields)