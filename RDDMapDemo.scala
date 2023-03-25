package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession


object RDDMapDemo {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("MapDemo")
        .master("local[1]")
        .getOrCreate()
      val data = Seq("sam","lahasya","deepak","sujith")

      import spark.sqlContext.implicits._

      val df = data.toDF("data")
      df.show()

      val mapDF = df.flatMap(fun=>{
        fun.getString(0).split(" ")
      })
      mapDF.show()
  }
}
