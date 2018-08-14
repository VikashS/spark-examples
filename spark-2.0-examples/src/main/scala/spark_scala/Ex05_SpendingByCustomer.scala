package spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Ex05_SpendingByCustomer extends App {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val userId = fields(0).toInt
    val spent = fields(2).toFloat
    (userId, spent)
  }

  //Set the log level to only print error
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Ex05_SpendingByCustomer").master("local[*]").getOrCreate()

  val sc = spark.sparkContext

  val lines = sc.textFile("/Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/customer-orders.csv")

  val usersTransactionsRDD = lines.map(parseLine)

  val userTotalSpending = usersTransactionsRDD.reduceByKey((x, y) => x + y)

  val sortBySpending = userTotalSpending.map(t => (t._2, t._1)).sortByKey()
  
  sortBySpending.collect().foreach(println)
}