package scala.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{ StructType, StructField, StringType };

object Ex03_LoadAndSaveJson1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Ex03_LoadAndSaveJson1")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val personRDD = sqlContext.jsonFile("data/person.json")

    val personDF = personRDD.toDF
    
    personDF.registerTempTable("person")
    
    val sixtyPlus = sqlContext.sql("select * from person where age > 60")
    
    sixtyPlus.toJSON.saveAsTextFile("data/sp")
    
    val person = sqlContext.read.json("data/person.json")
  }
}