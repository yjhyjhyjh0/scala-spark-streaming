import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf}

object KafkaDirectStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KafkaDirectStream")
      .setMaster("local[*]")

    val streamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "manual-offset-group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test-topic")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        // 1️⃣ Get offset ranges
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 2️⃣ Process data
        rdd.foreach { record: ConsumerRecord[String, String] =>
          println(s"key=${record.key()}, value=${record.value()}")
        }

        // 3️⃣ Manually commit offsets (to Kafka)
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}