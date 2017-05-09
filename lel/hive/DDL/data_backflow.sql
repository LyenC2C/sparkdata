CREATE TABLE  if not exists wl_base.t_base_datamart_backflow(
repeat String COMMENT '是否重复',
success String COMMENT '调用成功',
app_key String COMMENT '公司代号',
app_key_param String COMMENT '公司代号参数',
ts_create String COMMENT '日志记录时间戳',
ts_request String COMMENT '请求时间戳',
params_less map<string,string> COMMENT '请求参数，过滤掉公共参数',
params_all map<string,string> COMMENT '请求参数',
sign String COMMENT '标志',
interface String COMMENT '接口',
match String COMMENT '是否匹配',
api_type String COMMENT '调用api类型'
)
COMMENT '数据集市回流分析'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\001"  COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ":" stored as textfile;

load data inpath '/user/lel/temp/data_backflow' into table t_base_datamart_backflow partition(ds='20170406')






CREATE TABLE  if not exists wl_base.t_base_record_data_backflow(
repeat String COMMENT '是否重复',
success String COMMENT '调用成功',
app_key String COMMENT '公司代号',
app_key_param String COMMENT '公司代号参数',
ts_create String COMMENT '日志记录时间戳',
ts_request String COMMENT '请求时间戳',
params map<string,string> COMMENT '请求参数',
result String COMMENT '请求参数',
sign String COMMENT '标志',
interface String COMMENT '接口',
match String COMMENT '是否匹配',
api_type String COMMENT '调用api类型'
)
COMMENT 'record数据回流分析'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\001"  COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ":" stored as textfile;


load data inpath '/user/lel/temp/data_backflow_75G' into table t_base_record_data_backflow partition(ds='20170505')
