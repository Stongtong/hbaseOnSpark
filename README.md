# hbaseOnSpark
该项目主要是通过kafka，将pb数据插入到hbase的过程，项目中提供了两种hbase插入方式：分别是bulkload和batch put，并且程序的配置文件在设计的时候已hbase table为中心，支持同表、多topic、多rowkey，多列簇以及多列的对应关系的支持
