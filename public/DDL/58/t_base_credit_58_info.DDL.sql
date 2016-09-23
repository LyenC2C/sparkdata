CREATE  TABLE  if not exists t_base_credit_58_info (
t_action        string   comment '未知',
cateid        string   comment '类目id',
catename      string   comment '类目名称',
decrypted_tel      string   comment '手机号',
infoid        string   comment '发帖id',
isbiz         string   comment '未知',
nickname      string   comment '用户昵称',
online        string   comment '是否在线',
rootcateid         string   comment '根类目',
title         string   comment '帖子title',
tradeline          string   comment '交易线',
uid      string   comment '用户id',
uname         string   comment '来源名称-猜测应该是员工工号之类'
)
COMMENT '58发帖数据'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

-- LOAD DATA  INPATH '/user/zlj/data/dim.txt' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;
-- LOAD DATA  INPATH '/commit/t_base_ec_dim' OVERWRITE INTO TABLE t_base_ec_dim PARTITION (ds='20151023') ;

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