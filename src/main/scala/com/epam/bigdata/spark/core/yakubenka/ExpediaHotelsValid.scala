package com.epam.bigdata.spark.core.yakubenka

import org.apache.spark.{SparkConf, SparkContext, _}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.Properties
import java.time.Instant

import collection.JavaConverters._
import collection.JavaConversions._
import scala.util.parsing.json._

object ExpediaHotelsValid{

  val APP_NAME = "SparkBatchProcess"
  val LOG = LoggerFactory.getLogger(ExpediaHotelsValid.getClass)

  def main(args: Array[String]): Unit = {
    val inputTopic = args(0)
    val kafkaBrokers = args(1)
    val pathToExpediaHDFS = args(2)
    val pathToHotels = args(3)
    val outputPath = args(4)

    val conf = new SparkConf().setAppName(APP_NAME)
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.config(conf).appName(APP_NAME).getOrCreate()
    val sqlContext = sparkSession.sqlContext

    processData(sparkSession, sc, sqlContext, inputTopic, kafkaBrokers, pathToExpediaHDFS, pathToHotels, outputPath)
    sc.stop()
  }

  def processData(ss: SparkSession, sc: SparkContext, sqlContext : SQLContext, inputTopic: String, kafkaBrokers: String,
                  pathToExpediaHDFS: String, pathToHotels: String, outputPath: String) = {

    val hotelWeather: DataFrame = getHotelWeather(ss, sc, inputTopic, kafkaBrokers)

    val expedia: DataFrame = getExpedia(sqlContext, pathToExpediaHDFS)

    val hotelIdleDays: DataFrame = calcHotelIdleDays(ss, expedia)

    val validExpedia: DataFrame = getValidExpedia(ss, expedia, hotelIdleDays)

    val hotels: DataFrame = getHotels(sqlContext, pathToHotels)

    printInvalidHotels(ss, hotels, hotelIdleDays)

    printBookingsCounts(ss, validExpedia, hotels)

    storeValidExpedia(ss, validExpedia, outputPath)
  }

  def getHotelWeather(ss: SparkSession, sc: SparkContext, inputTopic: String, kafkaBrokers: String): DataFrame = {
    import ss.implicits._
    val gid = APP_NAME + Instant.now.getEpochSecond

    val kafkaParams = scala.collection.immutable.Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> gid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ).asJava

    val KafkaConfig = new Properties()
    KafkaConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    val adminClient = AdminClient.create(KafkaConfig)

    val consumer = new KafkaConsumer[String,String](kafkaParams)

    val topicPartitions = adminClient
      .describeTopics(List[String](inputTopic).asJava)
      .all().get().get(inputTopic).partitions()

    val offsetRanges = topicPartitions.asScala.map(x =>{
      val topicPartition = new TopicPartition(inputTopic, x.partition)
      val startOffset = consumer.beginningOffsets(List[TopicPartition](topicPartition))
        .values().asScala.toList.get(0)
      val stopOffset = consumer.endOffsets(List[TopicPartition](topicPartition))
        .values().asScala.toList.get(0)
      OffsetRange(topicPartition,
        startOffset,
        stopOffset)
    }).toArray

    val messagesRDD = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      offsetRanges, PreferConsistent)

    messagesRDD.map(message =>{
      val value = message.value().asInstanceOf[String]
      val msgValues = JSON.parseFull(value).get.asInstanceOf[Map[String, String]]
      (msgValues("id"),msgValues("name"),msgValues("country"),msgValues("city"),msgValues("address"),
        msgValues("avgtmprf").toDouble, msgValues("avgtmprc").toDouble, msgValues("wthrdate"), msgValues("precision").toInt)
    }).toDF("hotel_id","hotel_name","hotel_country","hotel_city", "hotel_address", "avg_tmpr_f", "avg_tmpr_c",
    "wthr_date", "precision")
  }

  def getExpedia(sqlContext: SQLContext, path: String) = {
    sqlContext.read
      .format("com.databricks.spark.avro")
      .load(path)
  }

  def calcHotelIdleDays(sparkSession: SparkSession, expedia: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    expedia.groupBy("hotel_id")
      .agg(((datediff(max("srch_ci"), min("srch_ci")) + 1) - countDistinct("srch_ci"))
        .as("idle_days"))
  }

  def calcHotelIdleDayss(sparkSession: SparkSession, expedia: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    expedia.withColumn("year", year(col("srch_ci"))).withColumn("month", month(col("srch_ci")))
      .groupBy("hotel_id", "year", "month")
      .agg(((datediff(max("srch_ci"),
        min("srch_ci")) + 1) - countDistinct("srch_ci")).as("idle_days") )
      .groupBy("hotel_id").agg(sum("idle_days").as("idle_days"))
  }

  def getValidExpedia(ss: SparkSession, expedia: DataFrame, hotelIdleDays: DataFrame): DataFrame = {
    import ss.sqlContext.implicits._
    expedia.join(hotelIdleDays, "hotel_id")
      .filter(!($"idle_days" >= 2 && $"idle_days" < 30))
      .as("validExpedia")
  }

  def getHotels(sqlContext: SQLContext, path: String) = {
      sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(path)
      .as("hotels")
  }

  def printInvalidHotels(ss: SparkSession, hotels: DataFrame, hotelsIdleDays: DataFrame) = {
    import ss.sqlContext.implicits._
    val invalidHotels = hotelsIdleDays
      .filter($"idle_days" >= 2 && $"idle_days" < 30).as("invalidHotels")
    invalidHotels.join(hotels, $"invalidHotels.hotel_id" === $"hotels.id")
      .collect.foreach(println)
  }

  def printBookingsCounts(ss: SparkSession, validExpedia: DataFrame, hotels: DataFrame) = {
    import ss.sqlContext.implicits._
    val validExpediaHotels = validExpedia.join(hotels, $"validExpedia.hotel_id" === $"hotels.id")
    validExpediaHotels.groupBy("country").count().collect.foreach(println)
    validExpediaHotels.groupBy("city").count().collect.foreach(println)
  }

  def storeValidExpedia(ss: SparkSession, validExpedia: DataFrame, path: String) = {
    import org.apache.spark.sql.functions._
    val validExpediaWithYear = validExpedia.withColumn("year", year(col("srch_ci")))
    validExpediaWithYear.write.partitionBy("year").parquet(path)
  }
}
