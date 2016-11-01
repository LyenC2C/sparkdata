


SELECT
weibotext
from
t_base_weibo_blackuser
where  length(regexp_replace
                      (weibotext,
                          '贷款|套现|银行流水|抵押|社保|信用卡|分期|提现|花呗|借呗|京东白条|易贷网|无担保|不需担保|逾期|不良记录|秒批|秒到|提额|降额|征信|黑户|身份证抠图'
                       ,'')
                     )<>length(weibotext)
                     limit 100;



-- 13635

SELECT
COUNT(1)
from
t_base_weibo_blackuser
where  length(regexp_replace
                      (weibotext,
                          '贷款|套现|银行流水|抵押|社保|信用卡|分期|提现|花呗|借呗|京东白条|易贷网|无担保|不需担保|逾期|不良记录|秒批|秒到|提额|降额|征信|黑户|身份证抠图'
                       ,'')
                     )<>length(weibotext)