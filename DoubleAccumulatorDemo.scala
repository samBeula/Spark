package com.scalaspark.exercises
import org.apache.spark.SparkContext

object DoubleAccumulatorExample {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "DoubleAccumulatorExample")
    val doubleAcc = sc.doubleAccumulator("MyDoubleAccumulator")
    val rdd = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
    rdd.foreach(x => doubleAcc.add(x))
    println("Double accumulator value: " + doubleAcc.value)
    sc.stop()

  }
}