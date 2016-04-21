package scala.examples.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Ex01_SchemaWithCaseClass {

  case class Person(first_name: String, last_name: String, age: Int)

  def main(args: Array[String]): Unit = {

    // create  a spark conf and context
    val conf = new SparkConf().setMaster("local").setAppName("Ex01_SchemaWithCaseClass")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val p = sc.textFile("data/hive/person/person.txt")

    //Split each line into an array of string, based on a comma,
    val pmap = p.map(line => line.split(","))

    //Convert the RDD of Array[String] into the RDD of Person case objects:
    val personRDD = pmap.map(p => Person(p(0), p(1), p(2).toInt))

    //Convert the personRDD into the personDF DataFrame:
    val personDF = personRDD.toDF

    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    personDF.registerTempTable("person")

    //display schema
    personDF.printSchema
    
    //show contents
    personDF.show
    
    //Run simple query    
    val people = sqlContext.sql("select * from person")

    //Get the output values from persons
    people.collect.foreach(println)

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT first_name, last_name, age FROM person WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    println("Result of RDD.map:")
    teenagers.map(row => s"first_name: ${row(0)}, last_name: ${row(1)}, age: ${row(2)}").collect().foreach(println)

    // or by field name:
    teenagers.map(row => "first_name: " + row.getAs[String]("first_name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[Any](List("first_name", "last_name", "age"))).collect().foreach(println)

  }
}