select *
from wlbase_dev.t_base_credit_58_info a
where length(regexp_replace(a.title ,'陆金所|红岭创投|微贷网|聚宝匯|聚宝汇|宜贷网|爱钱进|宜人贷|团贷网|翼龙贷|你我贷|嘉卡贷|搜易贷|好贷宝|有利网|拿去花|人人贷|温商贷|开鑫贷|博金贷|365易贷|珠宝贷|新新贷|和信贷|连资贷|捷信分期|诚信贷|量化派|微粒贷|拍拍贷|拍拍满|贷款|套现|银行流水|抵押|社保|信用卡|提现|借呗|京东白条|平安易贷|任性付|易贷网|无担保|不需担保|逾期|不良记录|秒批|秒到|提额|降额|征信|黑户|解套回款|身份证抠图|无担保|无视黑白|无征信|无视征信|黑户贷款|黑户下卡|贷款包装|纯白户面皮|白户贷款|电核流水|征信代打|无视网黑|无视黑白|代做企业对公流水账|代做普通流水账|工资流水|包过电核|闪电认证|企业邮箱|闪电认证|单位邮箱',''))
<>length(a.title)
and catename in ('投资担保','咨询','二手回收','保险','财务会计/评估','工商注册','网站建设/推广','特色加盟','广告传媒','制卡','代办签证/签注','家装服务','礼品定制','网络服务加盟',)