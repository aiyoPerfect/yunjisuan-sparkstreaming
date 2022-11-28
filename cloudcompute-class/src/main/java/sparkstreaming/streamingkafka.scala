package sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object streamingkafka {

  def main(args: Array[String]): Unit = {
    //TODO创建环境对象

    val sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //kafka
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "worker1:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "cloudcompute",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //读取kafka数据创建DStream
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("wordcount"), kafkaPara)
    )
    kafkaDataDS.print()
    //将每条消息的KV取出
    val valueDStream: DStream[String] = kafkaDataDS.map(record => record.value())

    //计算wordcount
    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}