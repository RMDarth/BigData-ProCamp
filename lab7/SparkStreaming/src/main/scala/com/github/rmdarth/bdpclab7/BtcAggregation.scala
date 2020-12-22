package com.github.rmdarth.bdpclab7

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object BtcAggregation {

  def process(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._

    // declare json schema for each flow-file
    val schema = new StructType().
      add("data", new StructType()
        .add("datetime", StringType)
        .add("amount", DoubleType)
        .add("price", DoubleType))

    input
      // parse json
      .select(from_json($"value".cast(StringType), schema).as("btc"))
      .withColumn("timestamp", $"btc.data.datetime".cast(LongType).cast(TimestampType))
      .withColumn("sales", $"btc.data.amount" * $"btc.data.price")
      // add window and watermark
      .withWatermark("timestamp", "3 minutes")
      .groupBy(window($"timestamp", "1 minute"))
      // aggregate
      .agg(
        count("btc.data.price").as("Count"),
        avg("btc.data.price").as("AvgPrice"),
        sum("sales").as("SalesTotal"))
  }

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

    // read data from stream
    val input = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topicName)
      .load()

    // process and output
    process(input)
      // write data
      .writeStream
      .format("json")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .option("checkpointLocation", "/bdpc/lab7/checkpoint")
      .start("gs://"+ bucketName +"/" + folderName)
      .awaitTermination()

    spark.close()
  }
}