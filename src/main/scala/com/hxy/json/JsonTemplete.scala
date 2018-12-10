package com.hxy.json

import com.fasterxml.jackson.annotation.JsonProperty

import scala.beans.BeanProperty

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/11/28
  */

case class HBaseConfig(@JsonProperty("hbase.zookeeper.quorum") @BeanProperty zooQuorum: String,
                       @JsonProperty("hbase.zookeeper.property.clientPort") @BeanProperty clientPort: String,
                       @JsonProperty("mapreduce.output.fileoutputformat.outputdir") @BeanProperty outDir: String)

case class KafkaConfig(@JsonProperty("bootstrap.servers") @BeanProperty servers: String,
                       @JsonProperty("key.deserializer") @BeanProperty keyDes: String,
                       @JsonProperty("value.deserializer") @BeanProperty valueDes: String,
                       @JsonProperty("group.id") @BeanProperty group: String,
                       @JsonProperty("auto.offset.reset") @BeanProperty offset: String,
                       @JsonProperty("enable.auto.commit") @BeanProperty autoCommit: java.lang.Boolean)

case class SparkConfig(@JsonProperty("app.name") @BeanProperty appName: String,
                       @JsonProperty("app.master") @BeanProperty master: String,
                       @JsonProperty("spark.streaming.batchDuration") @BeanProperty duration: Long)

case class Config(@JsonProperty("hbase") @BeanProperty hBaseConfig: HBaseConfig,
                  @JsonProperty("kafka") @BeanProperty kafkaConfig: KafkaConfig,
                  @JsonProperty("spark") @BeanProperty sparkConfig: SparkConfig)


case class ColumnFamily(@JsonProperty("family") @BeanProperty columnFamily: String,
                        @JsonProperty("columns") @BeanProperty columns: Array[String])

case class Row(@JsonProperty("row") @BeanProperty row: String,
               @JsonProperty("column.family") @BeanProperty columnFamily: Array[ColumnFamily])

case class TableInfo(@JsonProperty("table") @BeanProperty tableName: String,
                     @JsonProperty("topics") @BeanProperty topics: Array[String],
                     @JsonProperty("rows") @BeanProperty rowKeys: Array[Row])

case class TablesInfo(@JsonProperty("tables") @BeanProperty tables: Array[TableInfo])



