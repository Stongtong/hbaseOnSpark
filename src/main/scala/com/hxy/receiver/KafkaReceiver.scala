package com.hxy.receiver

import com.google.protobuf.AbstractMessageLite
import com.hxy.json.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @author tong tong
  *         descriptor ****
  *         create time 2018/11/26
  */
class KafkaReceiver[K, NV <: AbstractMessageLite] {
  def receive[V](kafkaConfig: KafkaConfig,
                 topics: Array[String],
                 ssc: StreamingContext,
                 recordProcessor: (K, V) => (K, NV)): DStream[(K, NV)] = {
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConfig.servers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> kafkaConfig.keyDes,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> kafkaConfig.valueDes,
      ConsumerConfig.GROUP_ID_CONFIG -> kafkaConfig.group,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> kafkaConfig.offset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> kafkaConfig.autoCommit)
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[K, V](topics, kafkaParams))
    val result = stream.map(record => (record.key, record.value)).map(kv => recordProcessor(kv._1, kv._2))
    result
  }

}
