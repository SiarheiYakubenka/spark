package com.epam.bigdata.spark.core.yakubenka

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.{FlatSpec}

class ExpediaHotelsValidTest extends FlatSpec with SparkSessionTestWrapper with DatasetComparer {
  import sparkSession.implicits._

  "testCalcHotelIdleDays" should "calculate idle days" in {
    val expedia = Seq(
      ("2", "2016-10-02"),
      ("2", "2016-10-06"),
      ("2", "2016-10-02"),
      ("2", "2016-10-03"),
      ("2", "2016-10-06"),
      ("2", "2016-10-04"),
      ("2", "2016-10-05"),
      ("2", "2016-10-04"),
      ("2", "2016-10-05"),
      ("2", "2016-10-04"),
      ("2", "2016-10-02"),
      ("2", "2016-10-06"),
      ("2", "2016-10-06"),
      ("2", "2016-10-09"),
      ("3", "2016-10-02"),
      ("3", "2016-10-06"),
      ("3", "2016-10-02"),
      ("3", "2016-10-03"),
      ("3", "2016-10-06"),
      ("3", "2016-10-04"),
      ("3", "2016-10-05"),
      ("3", "2016-10-04"),
      ("3", "2016-10-05"),
      ("3", "2016-10-04"),
      ("3", "2017-10-02"),
      ("3", "2017-10-03")
    ).toDF("hotel_id", "srch_ci")

    val hotels = Seq(
      ("2", "Name2", "Address2", "country2", "city2"),
      ("3", "Name3", "Address3", "country3", "city3")
    ).toDF("id", "name", "address", "country", "city").as("hotels")

    val expected = Seq(
      ("3", 0),
      ("2", 2)
    ).toDF("hotel_id", "sum(idle_days)")

    expedia.show()

    val actualDF =  ExpediaHotelsValid.calcHotelIdleDayss(sparkSession, expedia)
    actualDF.show()

  }

  "testvalidExpedia" should "validate Expedia" in {
    val expedia = Seq(
      ("2", "2016-10-02"),
      ("2", "2016-10-06"),
      ("2", "2016-10-02"),
      ("2", "2016-10-03"),
      ("2", "2016-10-06"),
      ("2", "2016-10-04"),
      ("2", "2016-10-05"),
      ("2", "2016-10-04"),
      ("2", "2016-10-05"),
      ("2", "2016-10-04"),
      ("2", "2016-10-02"),
      ("2", "2016-10-06"),
      ("2", "2016-10-06"),
      ("2", "2016-10-09"),
      ("3", "2016-10-02"),
      ("3", "2016-10-06"),
      ("3", "2016-10-02"),
      ("3", "2016-10-03"),
      ("3", "2016-10-06"),
      ("3", "2016-10-04"),
      ("3", "2016-10-05"),
      ("3", "2016-10-04"),
      ("3", "2016-10-05"),
      ("3", "2016-10-04"),
      ("3", "2017-10-02"),
      ("3", "2017-10-03")
    ).toDF("hotel_id", "srch_ci")

    val hotels = Seq(
      ("2", "Name2", "Address2", "country2", "city2"),
      ("3", "Name3", "Address3", "country3", "city3")
    ).toDF("id", "name", "address", "country", "city").as("hotels")

    val idle = Seq(
      ("3", 0),
      ("2", 2)
    ).toDF("hotel_id", "idle_days")

    expedia.show()

    val actualDF =  ExpediaHotelsValid.getValidExpedia(sparkSession, expedia, idle)
    actualDF.show()

  }

}
