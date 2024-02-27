package keith_scala_spark

import org.apache.spark.sql.SparkSession

object MySparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // Example data
    val dataSeq = Seq(("Alice", 1), ("Bob", 2), ("Charlie", 3))
    val df = dataSeq.toDF("name", "id")

    // Collecting DataFrame to Array and printing
    df.collect().foreach { row =>
      println(s"name: ${row.getAs[String]("name")}, id: ${row.getAs[Int]("id")}")
    }

    spark.stop()
  }
}