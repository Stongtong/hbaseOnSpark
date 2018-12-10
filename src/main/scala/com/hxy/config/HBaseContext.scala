package com.hxy.config

import com.hxy.json.HBaseConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapreduce.Job

/**
  * @author tongtong
  *         descriptor ****
  *         create time 2018/11/26
  */
class HBaseContext(hBaseConfig: HBaseConfig, inTable: String, outTable: String) extends Serializable {

  val config: Configuration = HBaseConfiguration.create
  config.set("hbase.zookeeper.quorum", hBaseConfig.zooQuorum)
  config.set("hbase.zookeeper.property.clientPort", hBaseConfig.clientPort)
  config.set(TableInputFormat.INPUT_TABLE, inTable)
  config.set("mapreduce.output.fileoutputformat.outputdir", hBaseConfig.outDir)

  def getJob: Job = {
    val job = Job.getInstance(config)
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, outTable)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job
  }

  def getConfig: Configuration = config

  def getTable: HTable = {
    val connHbase = ConnectionFactory.createConnection(config)
    connHbase.getTable(TableName.valueOf(outTable)).asInstanceOf[HTable]
  }
}

object HBaseContext {
  def apply(hBaseConfig: HBaseConfig, inTable: String, outTable: String): HBaseContext = new HBaseContext(hBaseConfig, inTable, outTable)
}
