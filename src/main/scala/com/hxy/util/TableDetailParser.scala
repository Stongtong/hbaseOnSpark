package com.hxy.util

import com.alibaba.fastjson.JSON
import com.google.protobuf.GeneratedMessage
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * @author tongtong
  *         descriptor **** 对rowkey进行解析映射为真实的rowkey
  *         create time 2018/12/6
  */
object TableDetailParser extends Serializable {

  val VIRTUAL_ROW_KEY_SPLITTER = "\\+"
  val EMPTY_PLACEHOLDER = " "

  def getAllFields[T <: GeneratedMessage](obj: T): mutable.HashMap[String, AnyRef] = {
    val map = mutable.HashMap[String, AnyRef]()
    val allField = obj.getAllFields
    val iterator = allField.entrySet.iterator
    while (iterator.hasNext) {
      val field = iterator.next
      map += field.getKey.getName -> field.getValue
    }
    map
  }

  def parseRowKey(fakeRow: String, valuesMap: mutable.HashMap[String, AnyRef]): Array[Byte] = {
    val builder = new mutable.StringBuilder()
    fakeRow.split(VIRTUAL_ROW_KEY_SPLITTER).foreach(field => {
      if (valuesMap.contains(field)) {
        val value = valuesMap(field)
        // todo 这个地方可以指定序列化方式，回来研究一下
        builder.append(value.toString)
      } else builder.append(field)
    })
    builder.toString().getBytes
  }

  def columnMapping(cfName: String, columns: Array[String], put: Put, valuesMap: mutable.HashMap[String, AnyRef]): Unit = {
    columns.foreach(column => {
      if (valuesMap.contains(column)) {
        val value = valuesMap(column)
        // todo 这个地方可以指定序列化方式，回来研究一下
        val valueByte = JSON.toJSONBytes(value)
        put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(column), valueByte)
      } else put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(column), Bytes.toBytes(EMPTY_PLACEHOLDER))
    })
  }
}
