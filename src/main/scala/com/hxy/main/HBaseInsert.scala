package com.hxy.main

import com.google.protobuf.GeneratedMessage
import com.hxy.json._
import com.hxy.put.TPut
import com.hxy.receiver.KafkaReceiver
import org.apache.spark.streaming.StreamingContext

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/11/28
  */
class HBaseInsert extends Serializable {
  def insert[K, V, T <: GeneratedMessage](config: Config, tablesInfo: Array[TableInfo], recordProcessor: (K, V) => (K, T), ssc: StreamingContext, tPut: TPut[K, T]): Unit =
    tablesInfo.foreach(tableInfo => {
      val insert = new KafkaReceiver[K, T]()
      val kafkaProcessorDStream = insert.receive(config.kafkaConfig, tableInfo.topics, ssc, recordProcessor)
      tPut.put(kafkaProcessorDStream, config.hBaseConfig, tableInfo)
    })
}

object HBaseInsert{
  def apply: HBaseInsert = new HBaseInsert()
}


