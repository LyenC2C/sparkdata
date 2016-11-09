insert overwrite table t_base_qqqun_unserinfo PARTITION(ds=20161210)
select tt1.qq_id,tt1.age,tt1.sex,tt2.realname from
(
select t1.qq_id as qq_id,t1.age as age,t2.sex as sex from
(
select qq_id, age from
(
select qq_id, age, ROW_NUMBER() OVER (PARTITION BY qq_id ORDER BY ct DESC) AS rn from
(select qq_id,field1 as age,count(1) as ct  from t_base_qq_qun_member_info group by qq_id,field1)tt
)t
WHERE
rn = 1
)t1
join
(
select qq_id, sex from
(
select qq_id, sex, ROW_NUMBER() OVER (PARTITION BY qq_id ORDER BY ct DESC) AS rn from
(select qq_id,field2 as sex,count(1) as ct from t_base_qq_qun_member_info group by qq_id,field2)ttt
)tttt
WHERE
rn = 1
)t2
ON
t1.qq_id = t2.qq_id
)tt1
left JOIN
t_base_qq_realname tt2
ON
tt1.qq_id = tt2.qq
