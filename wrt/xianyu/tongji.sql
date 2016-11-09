--统计省市区：
select count(1) FROM
(
select userid,province,city,area from t_base_ec_tb_xianyu_item
where province <> '' and province <> '-' and city <> ''and city <> '-' and area <> '' and area <> '-'
group by userid,province,city,area
)t

--统计省市区,小区：
select count(1) FROM
(
select userid,province,city,area,community_name from t_base_ec_tb_xianyu_item
where province <> '' and province <> '-' and city <> ''and city <> '-' and area <> '' and area <> '-'
and community_name <> '' and community_name <> '-'
group by userid,province,city,area,community_name
)t


--统计省gps：
select count(1) FROM
(
select userid,gps from t_base_ec_tb_xianyu_item
where gps <> '' and gps <> '-'
group by userid,gps
)t

