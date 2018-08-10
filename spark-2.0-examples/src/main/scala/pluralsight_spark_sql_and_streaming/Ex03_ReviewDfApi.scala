package pluralsight_spark_sql_and_streaming

import org.apache.spark.sql.SparkSession

object Ex03_ReviewDfApi extends App {
    val spark = SparkSession
    .builder()
    .appName("Ex03_ReviewDfApi")
    .master("local[2]")
    .config("spark.driver.memory", "128m")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val usersList = List(("aahmed",100), ("leo_adnan",200))
  val df = spark.createDataFrame(usersList)
  
  val namedDF= df.toDF("UserName", "Score") 
  
  
}