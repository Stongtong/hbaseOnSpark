package com.hxy.put

import java.util

import com.google.protobuf.GeneratedMessage
import com.hxy.config.HBaseContext
import com.hxy.json.{ColumnFamily, HBaseConfig, TableInfo}
import com.hxy.util.TableDetailParser
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.spark.streaming.dstream.DStream

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/12/7
  */
class BatchPut[K, V <: GeneratedMessage] extends TPut[K, V] with Serializable {
  val MAX_PUT = 1000

  override def put(streaming: DStream[(K, V)], hBaseConfig: HBaseConfig, tableInfo: TableInfo): Unit = {
    tableInfo.rowKeys.foreach(rowInfo => streaming.foreachRDD(streamRDD => {
      streamRDD.foreach({
        val list = new util.LinkedList[Put]()
        kValue => {
          list add transferPut(rowInfo.columnFamily, rowInfo.row, kValue._2)
          if (list.size >= MAX_PUT) {
            val hBaseContext = new HBaseContext(hBaseConfig, tableInfo.tableName, tableInfo.tableName)
            val connHbase = ConnectionFactory.createConnection(hBaseContext.config)
            val hTable = connHbase.getTable(TableName.valueOf(tableInfo.tableName)).asInstanceOf[HTable]
            hTable put list
            list clear()
          }
        }
      })
    }))
  }

  private def transferPut(columnFamilies: Array[ColumnFamily], virtualRowKey: String, value: V): Put = {
    val valuesFieldMap = TableDetailParser.getAllFields(value)
    // parse row key
    val rowKey = TableDetailParser.parseRowKey(virtualRowKey, valuesFieldMap)
    val put = new Put(rowKey)
    columnFamilies.foreach(columnFamily => {
      val cfName = columnFamily.columnFamily
      val columns = columnFamily.columns
      TableDetailParser.columnMapping(cfName, columns, put, valuesFieldMap)
    })
    put
  }
}
