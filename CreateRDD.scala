package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MyFirstRDD")
      .master("local[1]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(("sam",1),("lahasya",2)))

    rdd.foreach(println)

    val a = scala.io.StdIn.readLine()
    println(a)
  }
}
