package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object TextFileDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD2")
      .master("local[1]")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("src/main/scala/com/scalaspark/exercises/text1.txt")

    rdd.foreach(println)


  }
}
