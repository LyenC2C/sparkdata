

--  LOAD DATA   INPATH '/user/zlj/tmp/sinawb_user_info.json.20160401' OVERWRITE INTO TABLE t_base_weibo_user PARTITION (ds='20160829')


LOAD DATA   INPATH '/user/zlj/tmp/all.iminfo.json_parse' OVERWRITE INTO TABLE t_base_credit_58_info PARTITION (ds='20161009')


select title  from
t_base_credit_58_info
where  length(regexp_replace
                      (title,
                          '刷单|贷款|套现|银行流水|抵押|社保|信用卡|分期|提现|花呗|借呗|京东白条|易贷网|无担保|不需担保|逾期|不良记录|秒批|秒到|提额|降额|征信|黑户|身份证抠图'
                       ,'')    )<>length(title) limit 100;

SELECT  * from (
SELECT  decrypted_tel,COUNT(1) num  from t_base_credit_58_info group by decrypted_tel
)t order by num desc limit 100;


3331289
select COUNT(DISTINCT  uname) from t_base_credit_58_info;

3202361
select COUNT(DISTINCT  decrypted_tel) from t_base_credit_58_info;

696790
select COUNT(DISTINCT  nickname) from t_base_credit_58_info;



  INSERT OVERWRITE table t_base_credit_58_userinfo PARTITION(ds='20161009')
  SELECT decrypted_tel,nickname,uid ,uname
  from t_base_credit_58_info where ds=20161009 group by decrypted_tel,nickname,uid ,uname ;


-- 有效手机号 9574013
  SELECT COUNT(1) from t_base_credit_58_userinfo where  LENGTH (decrypted_tel)=11