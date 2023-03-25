package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .appName("BroadcastDemo")
      .master("local[1]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Seq(("emp1","1000","USA","NY"),("emp2","2000","USA","TX"),("emp3","3000","IND","AP"),("emp4","20000","IND","TS"),("emp5","4000","AUS","QNS")))
    //inputRDD.foreach(println)

    val countries = Map(("USA","United States of America"),("IND","India"),("AUS","Australia"))

    val states = Map(("NY","New York"),("TX","Texas"),("AP","Andhra Pradesh"),("TS","Telangana"),("QNS","Queens Land"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val finalRDD = inputRDD.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (f._1,f._2,fullCountry,fullState)
    })

    println(finalRDD.collect().mkString("\n"))
    //scala.io.StdIn.readLine()



  }
}
