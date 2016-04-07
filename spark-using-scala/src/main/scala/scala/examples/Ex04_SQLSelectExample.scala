package scala.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Ex04_SQLSelectExample {

  case class Employee(employeeID: Int,
    lastName: String, firstName: String, title: String,
    birthDate: String, hireDate: String, city: String,
    state: String, zip: String, country: String,
    reportsTo: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("ParseCSV")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val employeeFile = sc.textFile("/Users/aahme25/Data_Files/NW-Employees-NoHdr.csv")

    println(s"${"="*100}")
    println("Employee File has %d Lines.".format(employeeFile.count()))

    val employees: RDD[Employee] = employeeFile.map(_.split(",")).
      map(e => Employee(employeeID = e(0).trim.toInt,
        lastName = e(1), firstName = e(2), title = e(3),
        birthDate = e(4), hireDate = e(5), city = e(6),
        state = e(7), zip = e(8), country = e(9),
        reportsTo = e(10)))

    
    println(employees.count)
    employees.toDF().registerTempTable("Employees")

    var result = sqlContext.sql("SELECT * from Employees")

    println(s"${"="*100}")
    result.foreach(println)
    
    result = sqlContext.sql("SELECT * from Employees WHERE state ='WA'")
    println(s"${"="*100}")
    result.foreach(println)
  }
}