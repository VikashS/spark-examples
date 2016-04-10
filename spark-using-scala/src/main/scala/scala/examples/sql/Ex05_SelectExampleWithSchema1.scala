package scala.examples.sql

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Ex05_SelectExampleWithSchema1 {
  def main(args: Array[String]) {

    // create  a spark conf and context
    val conf = new SparkConf().setMaster("local").setAppName("Ex05_SelectExampleWithSchema1")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    //Load the person data in an RDD:
    val p = sc.textFile("data/hive/person")

    //Split each line into an array of string, based on a comma,
    val pmap = p.map(line => line.split(","))

    //Convert the RDD of array[string] to the RDD of the Row objects:
    val personData = pmap.map(p => Row(p(0), p(1), p(2).toInt))

    //Create schema using the StructType and StructField objects. 
    //The StructField object takes parameters in the form of param name, param type, and nullability:
    val schema = StructType(
      Array(StructField("first_name",   StringType,  true),
            StructField("last_name",    StringType,  true),
            StructField("age",          IntegerType, true)))
            
    //Apply schema to create the personDF DataFrame      
    val personDF = sqlContext.createDataFrame(personData,schema)        
    
    //Register the personDF as a table:
    personDF.registerTempTable("person")
    
    val persons = sqlContext.sql("select * from person")
    
    persons.collect.foreach(println)
  }
}