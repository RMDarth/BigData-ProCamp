package com.github.rmdarth.bdpclab7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BtcAggregation {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: SparkStreaming <bucket_name> <folder_name> <kafka_topic>")
      sys.exit(2)
    }

    val bucketName = args(0);
    val folderName = args(1);
    val topicName = args(2);

    // init spark
    val spark = SparkSession
      .builder
      .appName("BDPC-Lab7-BtcAggregation")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    // declare json schema for each flowfile
    val schema = new StructType().
      add("data", new StructType()
        .add("price", DoubleType)
        .add("datetime", StringType))

    // read data from stream
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topicName)
      .load()
      // parse json
      .select(from_json($"value".cast(StringType), schema).as("btc"))
      .withColumn("timestamp", $"btc.data.datetime".cast(LongType).cast(TimestampType))
      // add window and watermark
      .withWatermark("timestamp", "3 minutes")
      .groupBy(window($"timestamp", "1 minute"))
      // aggregate
      .agg(
        sum("btc.data.price").as("TotalSum"),
        count(lit(1)).as("Count"))
      .withColumn("Avg", $"TotalSum" / $"Count")
      // output
      .writeStream
      .format("json")
      .option("checkpointLocation", "/Users/darth/Downloads/testBtcOutput/checkpoint")
      .start("gs://"+ bucketName +"/" + folderName)
      .awaitTermination()

    spark.close()
  }
}