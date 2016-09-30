CREATE  TABLE  if not exists t_base_credit_ppd_info (
yuqi_id string comment '逾列表编号',
uname string comment '逾期用户',
real_name string comment '用户真实姓名',
phone string comment '电话（后三位没有）',
user_id_number string comment '身份证（后四位没有）',
w_year string comment '此用户开始逾期时的年份',
borrow_date string comment '借款日期',
borrow_nper string comment '借款期数',
time_out_interest string comment '逾期本息',
time_out_days string comment '逾期天数'
)
COMMENT '拍拍贷逾期信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n';