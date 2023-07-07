import org.apache.spark.sql.{Row, SparkSession}

object RddToDFDemo {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD to DataFrame Example")
      .master("local[*]")
      .getOrCreate()

    // Create an RDD of Person objects
    val rdd = spark.sparkContext.parallelize(Seq(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 35)
    ))


    import spark.implicits._
    val df = rdd.toDF()


    df.show()


    spark.stop()
   }
}