create table wlcredit.t_wrt_shixin_person(
id string,
iname string comment '失信人名称',
casecode string comment '案号',
cardnum string comment '身份证号码',
businessentity string comment '执行法院',
courtname string comment '执行法院',
areaname string comment '省份',
partytypename string comment '类型号',
gistid string comment '执行依据文号',
regdate string comment '立案时间',
gistunit string comment '做出执行依据单位',
duty string comment '失信被执行人行为具体情形',
performance string comment '被执行人的履行情况',
disrupttypename string comment '生效法律文书确定的义务',
publishdate string comment '发布日期',
performedpart string,
unperformpart string
)
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

