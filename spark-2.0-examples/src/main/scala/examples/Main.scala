package examples

import org.apache.spark.sql.SparkSession

case class Person(name: String, age: BigInt)

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("spark://PCW1844946L:7077")
//      .master("local")
      .appName("my-spark-app")
//      .config("spark.some.config.option", "config-value")
      .getOrCreate()
    
//    val input = spark.sparkContext.textFile("file:////Users/aahme25/Data_Files/log.txt")
//    val filter = input.filter { x => x.contains("ERROR") }
//    filter.collect().foreach { println }
    
//    val jsonData = spark.read.json("/usr/local/java/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json")
//    jsonData.collect().foreach(println)
    
//    val tables = spark.catalog.listTables()
//    tables.collect().foreach { println }
    
    import spark.implicits._ 
    // Turn a generic DataFrame into a Dataset of Person
    val ds = spark.read.json("/usr/local/java/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json").as[Person]
    println(ds.columns.mkString(","))
    println(ds.schema)
    ds.explain(true)
    ds.map(_.name).collect.foreach { println }
    
  }
}
//spark-submit -c spark.logLineage=true --verbose --master spark --class examples.Main ./target/scala-2.10/spark2-examples--assembly.jar