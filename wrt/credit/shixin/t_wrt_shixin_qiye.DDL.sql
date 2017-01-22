create table wlcredit.t_wrt_shixin_person(
id string,
iname string,
casecode string,
cardnum string,
businessentity string,
courtname string,
areaname string,
partytypename string,
gistid string,
regdate string,
gistunit string,
duty string,
performance string,
disrupttypename string,
publishdate string,
performedpart string,
unperformpart string
)
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;
