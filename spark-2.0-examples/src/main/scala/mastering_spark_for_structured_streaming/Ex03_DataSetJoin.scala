package mastering_spark_for_structured_streaming

import org.apache.spark.sql.SparkSession

object Ex03_DataSetJoin extends App {
  case class User(id: Int, name: String, email: String, country: String)
  case class Transaction(userId: Int, product: String, cost: Double)

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Ex03_DataSetJoin")
    .getOrCreate()

  import spark.implicits._

  val users = (spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("file:///Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/users.csv")
    .as[User])

  val transactions = (spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("file:///Users/adnan/Personal-GitHub/spark-examples/spark-2.0-examples/src/main/resources/transactions.csv")
    .as[Transaction])

  users.join(transactions, users.col("id") === transactions.col("userid"))
    .groupBy($"name")
    .sum("cost")
    .show
}