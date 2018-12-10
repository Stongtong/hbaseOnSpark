package com.hxy.put

import com.hxy.json.{HBaseConfig, Row, TableInfo}
import org.apache.spark.streaming.dstream.DStream

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/12/7
  */
trait TPut[K, V] {
  def put(streaming: DStream[(K, V)], hBaseConfig: HBaseConfig, tableInfo: TableInfo)
}
