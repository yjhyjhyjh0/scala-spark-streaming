# scala-spark-streaming
Example of scala streaming read kafka source.

A1- Spark structure streaming
spark.readStream .format("kafka")

A2- Direct stream read as rdd and manually handle offsets
KafkaUtils.createDirectStream 

## Build with 
```
Scala 2.12.12
Apache Spark 3.5.0
Kafka 3.9.2
```


