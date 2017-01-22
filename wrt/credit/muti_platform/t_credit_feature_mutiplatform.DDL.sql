create table wlcredit.t_credit_feature_mutiplatform
(
tel string,
features String
)
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n';