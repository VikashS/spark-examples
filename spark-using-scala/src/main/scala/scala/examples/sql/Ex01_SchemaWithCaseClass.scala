package scala.examples.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Ex01_SchemaWithCaseClass {
  
  case class Person(first_name:String,last_name:String,age:Int)

  def main(args: Array[String]): Unit = {

    // create  a spark conf and context
    val conf = new SparkConf().setMaster("local").setAppName("Ex01_SchemaWithCaseClass")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._                 
    
    val p = sc.textFile("data/hive/person/person.txt")
    
    //Split each line into an array of string, based on a comma,
    val pmap = p.map( line => line.split(","))
    
    //Convert the RDD of Array[String] into the RDD of Person case objects:
    val personRDD = pmap.map( p => Person(p(0),p(1),p(2).toInt))
    
    //Convert the personRDD into the personDF DataFrame:
    val personDF = personRDD.toDF
    
    //Register the personDF as a table
    personDF.registerTempTable("person")
    
    //Run a SQL query against it:
    val people = sqlContext.sql("select * from person")
    
    //Get the output values from persons
    people.collect.foreach(println)
  }
}
