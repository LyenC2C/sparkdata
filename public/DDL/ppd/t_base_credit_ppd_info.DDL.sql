CREATE  TABLE  if not exists t_base_credit_ppd_info (
uname string comment '拍拍贷id',
real_name string comment '真实姓名',
phone string comment '电话（后三位没有）',
user_id_number string comment '身份证（后四位没有）',
borrow_list string comment '贷款记录（json格式）',
w_year string comment '违约年份'
)
COMMENT '拍拍贷个人用户信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n';