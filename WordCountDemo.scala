package com.scalaspark.exercises

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("WordCountDemo.com")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("src/main/scala/com/scalaspark/exercises/text2.txt")
    println("initial partition count:" + rdd.getNumPartitions)

    val reparRdd = rdd.repartition(4)
    println("re-partition count:" + reparRdd.getNumPartitions)

    

    rdd.collect().foreach(println)

    
    val rdd2 = rdd.flatMap(f => f.split(" "))
    rdd2.foreach(f => println(f))

    
    val rdd3: RDD[(String, Int)] = rdd2.map(m => (m, 1))
    rdd3.foreach(println)

    
    val rdd4 = rdd3.filter(a => a._1.startsWith("a"))
    rdd4.foreach(println)

    
    val rdd5 = rdd3.reduceByKey(_ + _)
    rdd5.foreach(println)

    
    val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()
    println("Final Result")

    
    rdd6.foreach(println)

    
    println("Count : " + rdd6.count())

    
    val firstRec = rdd6.first()
    println("First Record : " + firstRec._1 + "," + firstRec._2)

    
    val datMax = rdd6.max()
    println("Max Record : " + datMax._1 + "," + datMax._2)

    
    val totalWordCount = rdd6.reduce((a, b) => (a._1 + b._1, a._2))
    println("dataReduce Record : " + totalWordCount._1)
    
    val data3 = rdd6.take(3)
    data3.foreach(f => {
      println("data3 Key:" + f._1 + ", Value:" + f._2)
    })

    
    val data = rdd6.collect()
    data.foreach(f => {
      println("Key:" + f._1 + ", Value:" + f._2)
    })

    
    rdd5.saveAsTextFile("c:/tmp/wordCount")
  }
}
