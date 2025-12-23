
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaStructuredStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaStructuredStreamingExample")
      .master("local[*]") // local test
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Read from Kafka
    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "test-topic")
      .option("startingOffsets", "earliest")
      .load()

    /*
      Kafka schema:
      key: binary
      value: binary
      topic: string
      partition: int
      offset: long
      timestamp: timestamp
      timestampType: int
    */

    val parsedDf = kafkaDf.select(
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp"),
      col("key").cast("string").as("key"),
      col("value").cast("string").as("value")
    )

    val query = parsedDf.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .option("checkpointLocation", "spark-kafka-checkpoint")
      .start()

    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+----------+---------+------+-----------------------+----+-----------+
    //|topic     |partition|offset|timestamp              |key |value      |
    //+----------+---------+------+-----------------------+----+-----------+
    //|test-topic|0        |0     |2025-12-19 15:48:13.558|NULL|{"c1":"v1"}|
    //|test-topic|0        |1     |2025-12-21 16:11:27.023|0   |{"c2":"v0"}|
    //|test-topic|0        |2     |2025-12-21 16:11:27.225|1   |{"c2":"v1"}|
    //|test-topic|0        |3     |2025-12-21 16:11:27.43 |2   |{"c2":"v2"}|
    //|test-topic|0        |4     |2025-12-21 16:11:27.636|3   |{"c2":"v3"}|
    //|test-topic|0        |5     |2025-12-21 16:11:27.838|4   |{"c2":"v4"}|
    //+----------+---------+------+-----------------------+----+-----------+

    query.awaitTermination()
  }
}