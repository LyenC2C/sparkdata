
SELECT count(1) from t_zlj_credit_valid_api1_step1 ;


create table t_zlj_credit_valid_api1_step1 as
SELECT
  tel,
  qqid,
  real_name,
  weibo_id,
  qqweibo,
  tb_id,
  email,
  tb_score ,
  tb_regyear
FROM
  (
    SELECT
      tel,
      qqid,
      real_name,
      weibo_id,
      qqweibo,
      t1.tb_id,
      email,
      score AS                  tb_score,
      year  AS                  tb_regyear,
      row_number()
      OVER (PARTITION BY t1.tel
        ORDER BY t2.score DESC) rank
    FROM
      (
          SELECT
          uid tel,
          id1 qqid,
          id2 real_name,
          id3 weibo_id,
          id4 qqweibo,
          id5 tb_id,
          id6 email FROM t_base_uid_tmp
          WHERE ds ='link' and   uid   rlike   '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
      ) t1

      JOIN
      (
        SELECT
          tb_id,year,
          (CASE WHEN tage IS NULL
            THEN 0
           ELSE 1 END +
           CASE WHEN tgender IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN city IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN alipay IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN year IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN buycnt IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN verify_level IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN ac_score_normal IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN sum_level IS NULL
             THEN 0
           ELSE 1 END +
           CASE WHEN feedrate IS NULL
             THEN 0
           ELSE 1 END) AS score
from t_pzz_tag_basic_info
      ) t2 ON t1.tb_id = t2.tb_id

  ) t1
WHERE rank = 1 ;



Drop table t_zlj_credit_valid_api1_step2 ;
create table t_zlj_credit_valid_api1_step2  as

SELECT  t1.*
,
  regtime as tb_regtime ,
  tb_nick,
  weibo_created_at  as weibo_regtime ,
  weibo_screen_name  as weibo_nick_name,
  xianyu_birthday as xianyu_info ,
  qq_name,
  58_tel ,
  58_nickname
from t_zlj_credit_valid_api1_step1  t1 join t_base_user_profile t2 on
  t1.tb_id=t2.tb_id ;


DROP TABLE  t_zlj_credit_valid_api1 ;
create table t_zlj_credit_valid_api1 as
  SELECT
    tb_id,
    case when tb_id  is not null and tb_id<>'-' then 1 else -1 end  as tb_flag ,
    tb_nick,
    tb_regtime ,
    case when xianyu_info  is not null and xianyu_info<>'-' then 1 else -1 end  as tb_xianyu_flag ,
    case when weibo_id  is not null and weibo_id<>'-' then 1 else -1 end  as snwb_id_flag ,
    weibo_nick_name ,
    weibo_regtime ,

    case when qqid  is not null and qqid<>'-' then 1 else -1 end  as qq_flag,
    qqid ,
    case when email  is not null and email<>'-' then 1 else -1 end  as email_flag ,
    email,
    case when 58_tel  is not null and 58_tel<>'-' then 1 else -1 end  as 58_flag,
    58_nickname
    from
    t_zlj_credit_valid_api1_step2
    ;




和信息相关的后两位打星号


tb_id	string
tb_flag	int	    淘宝是否注册
tb_nick	string	淘宝昵称
tb_regyear	double	淘宝注册时间
tb_xianyu_flag	int	淘宝咸鱼是否开通
snwb_id_flag	int	新浪微博是否开通
weibo_nick_name	string	新浪微博昵称
weibo_regtime	string	新浪微博注册时间
qq_flag	int	qq是否开通
qqid	string	qq 号
email_flag	int	邮箱是否注册
email	string	邮箱是否注册
58_flag	int	58是否注册
58_nickname	string	58昵称



tb_id                   string
tb_flag                 int
tb_nick                 string
tb_regtime              string
tb_xianyu_flag          int
snwb_id_flag            int
weibo_nick_name         string
weibo_regtime           string
qq_flag                 int
qqid                    string
email_flag              int
email                   string
58_flag                 int
58_nickname             string

