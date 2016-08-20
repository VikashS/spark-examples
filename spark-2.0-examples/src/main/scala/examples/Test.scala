package examples

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder().appName("Sample App").master("local").getOrCreate()
    
    val df = spark.read.option("header", "true").csv("src/main/resources/sales.csv")
    df.show()
    
    import spark.implicits._
    val data = spark.read.text("src/main/resources/data.txt").as[String]
    val words = data.flatMap(x=>x.split("\\s+"))
    val groupedWords = words.groupByKey { _.toLowerCase() }
    val counts = groupedWords.count()
    counts.show()
  }
}