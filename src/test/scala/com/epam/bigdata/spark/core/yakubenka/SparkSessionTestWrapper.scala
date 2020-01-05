package com.epam.bigdata.spark.core.yakubenka

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val sparkSession: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }
}