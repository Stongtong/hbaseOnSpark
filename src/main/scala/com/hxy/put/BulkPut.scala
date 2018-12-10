package com.hxy.put

import com.google.protobuf.GeneratedMessage
import com.hxy.config.HBaseContext
import com.hxy.json.{ColumnFamily, HBaseConfig, Row, TableInfo}
import com.hxy.util.TableDetailParser
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.streaming.dstream.DStream

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/12/7
  */
class BulkPut[K, V <: GeneratedMessage] extends TPut[K, V] with Serializable {

  override def put(streaming: DStream[(K, V)], hBaseConfig: HBaseConfig, tableInfo: TableInfo): Unit = {
    val hBaseContext = new HBaseContext(hBaseConfig, tableInfo.tableName, tableInfo.tableName)
    val job = hBaseContext.getJob
    tableInfo.rowKeys.foreach(rowInfo => streaming.foreachRDD(streamRDD => {
      val hBasePuts = streamRDD.map(kValue => transferPut(rowInfo.columnFamily, rowInfo.row, kValue._2))
      hBasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration)
    }))
  }

  private def transferPut(columnFamilies: Array[ColumnFamily], virtualRowKey: String, value: V): (ImmutableBytesWritable, Put) = {
    val valuesFieldMap = TableDetailParser.getAllFields(value)
    // 解析rowkey
    val rowKey = TableDetailParser.parseRowKey(virtualRowKey, valuesFieldMap)
    val put = new Put(rowKey)
    columnFamilies.foreach(columnFamily => {
      val cfName = columnFamily.columnFamily
      val columns = columnFamily.columns
      TableDetailParser.columnMapping(cfName, columns, put, valuesFieldMap)
    })
    (new ImmutableBytesWritable(), put)
  }
}
