package com.hxy.main

import com.hxy.json.{Config, TablesInfo}
import com.hxy.protobuf.DSFusion
import com.hxy.put.{BatchPut, BulkPut}
import com.hxy.util.Json2Object
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/11/30
  */
object HBaseAccess {
  def main(args: Array[String]): Unit = {
    // system configuration
    val globalConfig = Json2Object.parse1[Config]("/configure.json")
    val tableConfig = Json2Object.parse[TablesInfo]("/mapping.json")
    val sparkConfig = globalConfig.sparkConfig
    val conf = new SparkConf().setAppName(sparkConfig.appName)
      .setMaster(sparkConfig.master)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(sparkConfig.duration))

    def recordProcessor(key: Int, originalValue: Array[Byte]): (Int, DSFusion.DSFusionMessage) = (key, DSFusion.DSFusionMessage.parseFrom(originalValue))

//    HBaseInsert.apply.insert[Int, Array[Byte], DSFusion.DSFusionMessage](globalConfig, tableConfig.tables, recordProcessor, ssc, new BulkPut[Int, DSFusion.DSFusionMessage])
    HBaseInsert.apply.insert[Int, Array[Byte], DSFusion.DSFusionMessage](globalConfig, tableConfig.tables, recordProcessor, ssc, new BatchPut[Int, DSFusion.DSFusionMessage])
    ssc.start()
    ssc.awaitTermination()
  }
}
