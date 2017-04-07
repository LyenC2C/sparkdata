222	hive.auto.convert.join=true
223	hive.auto.convert.join.noconditionaltask=true
224	hive.auto.convert.join.noconditionaltask.size=20971520 ->20m
393	hive.mapjoin.optimized.hashtable=true
395	hive.mapjoin.smalltable.filesize=25000000



通过以下方法来在map执行前合并小文件，减少map数：
set mapred.max.split.size=240000000;
set mapred.min.split.size.per.node=240000000;
set mapred.min.split.size.per.rack=240000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
前面三个参数确定合并文件块的大小，大于文件块大小256m的，按照256m来分隔，小于256m,大于240m的，按照240m来分隔，把那些小于240m的（包括小文件和分隔大文件剩下的）进行合并
第四个参数表示执行前进行小文件合并



1.在Map-only的任务结束时合并小文件
2.在Map-Reduce的任务结束时合并小文件
3.合并文件的大小
4.当输出文件的平均大小小于该值时，启动一个独立的map-reduce任务进行文件merge
set hive.merge.mapfiles = true 
set hive.merge.mapredfiles = true
set hive.merge.size.per.task = 256*1000*1000 
set hive.merge.smallfiles.avgsize= 16000000 

set mapred.reduce.tasks = 1;


set hive.exec.reducers.bytes.per.reducer=256000000每个reduce任务处理的数据量）
set hive.exec.reducers.max=（每个任务最大的reduce数，默认为1009）
