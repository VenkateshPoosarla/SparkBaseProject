package sample

import org.apache.spark.sql.SparkSession

object TestSpark extends App {

  println("Spark Session is being created")

  val spark = SparkSession.builder().master("local").appName("SparkML").getOrCreate()

  println("Spark Session has created")

  import spark.implicits._

  val sqlContext = spark.sqlContext
  val sc = sqlContext.sparkContext
  sc.setLogLevel("WARN")
  val columns = Array("id", "first", "last", "year")
  val df1 = sc.parallelize(Seq(
    (1, "John", "Doe", 1986),
    (2, "Ive", "Fish", 1990),
    (4, "John", "Wayne", 1995)
  )).toDF(columns: _*)
  df1.show()
  val df2 = sc.parallelize(Seq(
    (1, "John", "Doe", 1986),
    (2, "IveNew", "Fish", 1990),
    (3, "San", "Simon", 1974)
  )).toDF(columns: _*)
  df2.show()
}
