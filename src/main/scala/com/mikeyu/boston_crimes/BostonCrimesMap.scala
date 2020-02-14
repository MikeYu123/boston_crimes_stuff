package com.mikeyu.boston_crimes

import org.apache.spark.sql.catalyst.expressions.aggregate.Percentile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BostonCrimesMap extends App with SparkSessionWrapper {
  import spark.implicits._

  val crimes = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(args(0)).dropDuplicates("INCIDENT_NUMBER" :: Nil)

  val offenseCodes = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(args(1)).dropDuplicates("CODE" :: Nil)

  val districtWindow = Window.partitionBy("DISTRICT")

  crimes
    .join(offenseCodes, $"OFFENSE_CODE" === $"CODE")
    .where($"DISTRICT".isNotNull)
    .withColumn("crime_name_shortcut", trim(element_at(split($"NAME", "-"), 1)))
    .withColumn("avgLat", avg("Lat").over(districtWindow))
    .withColumn("avgLong", avg("Long").over(districtWindow))
    .withColumn("crimes_total", count("*").over(districtWindow))
    .withColumn("crimes_this_month", count("*").over(Window.partitionBy("DISTRICT", "MONTH", "YEAR")))
    .withColumn("crimes_that_type", count("*").over(Window.partitionBy("CODE", "DISTRICT")))
    .withColumn("crimes_rank", dense_rank().over(Window.partitionBy("DISTRICT").orderBy(-$"crimes_that_type")))
    .where($"crimes_rank" < 4)
    .withColumn("frequent_crimes", array_join(collect_set($"crime_name_shortcut").over(districtWindow), ", "))
    .groupBy("DISTRICT", "MONTH")
    .agg(
      first($"crimes_total").as("crimes_total"),
      first($"crimes_this_month").as("crimes_this_month"),
      first($"avgLat").as("avgLat"),
      first($"avgLong").as("avgLong"),
      first($"frequent_crimes").as("frequent_crimes"))
    .groupBy("DISTRICT")
    .agg(
      callUDF("percentile_approx", $"crimes_this_month", lit(0.5)).as("crimes_monthly"),
      first($"crimes_total").as("crimes_total"),
      first("avgLat").as("lat"),
      first("avgLong").as("lng"),
      first("frequent_crimes").as("frequent_crime_types")
    )
    .withColumnRenamed("DISTRICT", "district")
    .repartition(1)
    .write
    .parquet(args(2))

}
