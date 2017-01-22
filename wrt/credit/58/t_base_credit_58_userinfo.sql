insert overwrite table wlbase_dev.t_base_credit_58_userinfo partition (ds = '20161208')
select
max(decrypted_tel) as decrypted_tel,
max(nickname) as nickname,
uid,
max(uname) as nickname
FROM
wlcredit.t_base_credit_58_info
where ds = 20161208
group by uid
