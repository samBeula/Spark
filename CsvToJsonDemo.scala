package com.scalaspark.exercises

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToJsonDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to JSON Example")
      .master("local[*]")
      .getOrCreate()


    val csvPath = "src/main/scala/com/scalaspark/exercises/emp.csv"
    val df = spark.read.format("csv")
      .option("header", "true")
      .load(csvPath)


    val jsonPath = "src/main/scala/com/scalaspark/exercises/emp.json"
    df.write.format("json")
      .mode(SaveMode.Overwrite)
      .save(jsonPath)


    spark.stop()
    }
}
