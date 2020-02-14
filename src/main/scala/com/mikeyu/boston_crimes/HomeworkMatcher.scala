package com.mikeyu.boston_crimes

import java.io.{File, PrintWriter}

import com.mikeyu.boston_crimes.BostonCrimesMap.spark
import org.apache.spark.sql.DataFrame
import spray.json._

import scala.util.Try

object HomeworkMatcher extends App with SparkSessionWrapper with DefaultJsonProtocol {
  case class CrimeSummary(district: String,
                          crimes_monthly: Long,
                          crimes_total: Long,
                          frequent_crime_types: String,
                          lat: Double,
                          lng: Double)
  
  case class CheckStatus(correct: Int, outOf: Int, incorrect: Set[String])
  case class TestResults(
                          correctRows: CheckStatus,
                          correctColumns: CheckStatus,
                          crimesMonthly: CheckStatus,
                          crimesTotal: CheckStatus,
                          frequentCrimeTypes: CheckStatus,
                          lat: CheckStatus,
                          lng: CheckStatus
                        )

  implicit val checkStatusFormat = jsonFormat3(CheckStatus)
  implicit val testResultFormat = jsonFormat7(TestResults)


  import spark.implicits._

  val reference = spark.read.parquet(args(0))
  val testData: DataFrame = spark.read.parquet(args(1))
  val refs = reference.as[CrimeSummary].collect.toSet

  def calculateResult(tests: Map[String, Boolean]) = {
    val res: (Set[String], Int) = tests.toList.map{
      case (_, true) => (None, 1)
      case (wrong, false) => (Some(wrong), 0)
    }.foldLeft[(Set[String], Int)] ((Set(), 0)) ((acc, v) => (v._1.fold(acc._1)(acc._1 + _), acc._2 + v._2))
    CheckStatus(res._2, tests.size, res._1)
  }

  def doubleEq(d1: Double, d2: Double) = {
    Math.abs(d1 - d2) < 0.001d
  }

  def getValue[T](district: String, column: String): T = {
    testData
      .where($"district" === district)
      .select(column)
      .first().get(0).asInstanceOf[T]
  }

//  check cols
  val correctCols = calculateResult(
    Set("district", "crimes_monthly", "crimes_total", "lat", "lng", "frequent_crime_types")
      .map(col => col -> testData.columns.contains(col)).toMap
  )


  // check rows
//  FIXME unreadable
  val correctRows = calculateResult(
      refs.map(_.district)
        .map(district =>
          district -> (Try{testData.where($"district" === district).count().toInt}.getOrElse(0) > 0)
        ).toMap
    )

//  check lat
  val correctLat = calculateResult(refs.map{ summary =>
    summary.district -> doubleEq(summary.lat, getValue(summary.district, "lat"))
  } toMap)

  //  check lng
  val correctLng = calculateResult(refs.map{ summary =>
    summary.district -> doubleEq(summary.lng, getValue(summary.district, "lng"))
  } toMap)
  
  val correctCrimesTotal = calculateResult(
    refs.map{ summary =>
      summary.district -> (summary.crimes_total == getValue[Long](summary.district, "crimes_total"))
    } toMap
  )

  val correctCrimesMonthly = calculateResult(
    refs.map{ summary =>
      summary.district -> (summary.crimes_monthly == getValue[Long](summary.district, "crimes_monthly"))
    } toMap
  )

  val correctFrequentCrimeTypes = calculateResult(
    refs.map{ summary =>
      summary.district -> (
        summary.frequent_crime_types.split(", ").toSet ==
          getValue[String](summary.district, "frequent_crime_types").split(", ").toSet)

    } toMap
  )

  val result = TestResults(correctRows,
    correctCols,
    correctCrimesMonthly,
    correctCrimesTotal,
    correctFrequentCrimeTypes,
    correctLat,
    correctLng)

  new PrintWriter(args(2)) {
    write(result.toJson.toString)
    close()
  }
}
