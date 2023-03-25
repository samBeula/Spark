package com.scalaspark.exercises

import org.apache.spark
import org.apache.spark.sql.SparkSession

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AccumulatorDemo")
      .master("local[1]")
      .getOrCreate()

    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
    val rdd = spark.sparkContext.parallelize(Array(1,2,3,4,5))
    rdd.foreach(x => longAcc.add(x))

    println(longAcc.value)
    //scala.io.StdIn.readLine()

  }
}
