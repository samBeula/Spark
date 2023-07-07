package com.scalaspark.exercises

import org.apache.spark.sql.{SaveMode, SparkSession}

object JsonToCsvDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JSON to CSV Example")
      .master("local[*]")
      .getOrCreate()


    val jsonPath = "src/main/scala/com/scalaspark/exercises/jsonFile"
    val df = spark.read.format("json")
      .load(jsonPath)


    val csvPath = "src/main/scala/com/scalaspark/exercises/csvFile"
    df.write.format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(csvPath)


    spark.stop()
    }
}
