package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object FirstDFDemo {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .appName("FirstDFDemo")
      .master("local[1]")
      .getOrCreate()
    val data = Seq(("Lahasya","1000",21,"Hyderabad"),
      ("daisy","2000",25,"Vizag"),
      ("sujith","3000",22,"parvathipuram"),
      ("deepak","4000",20,"Chandhragiri"),
      ("sam","5000",26,"Tuni"))
    val cols = Seq("name","salary","age","place")

    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(data)

    val df = rdd.toDF(cols:_*)
    df.show()
  }
}
