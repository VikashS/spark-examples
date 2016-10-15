package scala.examples.sql

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._

object Ex05_SelectExampleWithSchema2 {
def main(args: Array[String]) {

    // create  a spark conf and context
    val conf = new SparkConf().setMaster("local").setAppName("Ex05_SelectExampleWithSchema2")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rawRdd = sc.textFile("data/adult.txt")

    // specify a schema
    val schema =
      StructType(
        StructField("age",                IntegerType, false) :: 
        StructField("workclass",          StringType,  false) ::
        StructField("fnlwgt",             IntegerType, false) ::
        StructField("education",          StringType,  false) ::
        StructField("educational-num",    IntegerType, false) ::
        StructField("marital-status",     StringType,  false) ::
        StructField("occupation",         StringType,  false) ::
        StructField("relationship",       StringType,  false) ::
        StructField("race",               StringType,  false) ::
        StructField("gender",             StringType,  false) ::
        StructField("capital-gain",       IntegerType, false) ::
        StructField("capital-loss",       IntegerType, false) ::
        StructField("hours-per-week",     IntegerType, false) ::
        StructField("native-country",     StringType,  false) ::
        StructField("income",             StringType,  false) ::
        Nil)

    // create row data
    val rowRDD = rawRdd.map(_.split(" "))
      .map(p => Row( p(0).trim.toInt,p(1),p(2).trim.toInt,p(3),
                     p(4).trim.toInt,p(5),p(6),p(7),p(8),
                     p(9),p(10).trim.toInt,p(11).trim.toInt,
                     p(12).trim.toInt,p(13),p(14) ))

    // create a data frame from schema and row data
    val adultDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // now create a temporary table and run some sql examples
    adultDataFrame.registerTempTable("adult")

    // ---------------------------------------------------
    // example 1
     var df = sqlContext.sql("SELECT COUNT(*) FROM adult")

    // ---------------------------------------------------
    // example 2
//     df = sqlContext.sql("SELECT * FROM adult LIMIT 10")
//     df.map(t => t(0)  + " " + t(1)  + " " + t(2)  + " " + t(3)  + " " + 
//                     t(4)  + " " + t(5)  + " " + t(6)  + " " + t(7)  + " " + 
//                     t(8)  + " " + t(9)  + " " + t(10) + " " + t(11) + " " + 
//                     t(12) + " " + t(13) + " " + t(14) 
//               )
//       .collect().foreach(println)

    // ---------------------------------------------------
    // example 3
//     df = sqlContext.sql("SELECT COUNT(*) FROM adult WHERE age < 60")
//     df.map(t => "Count - " + t(0)).collect().foreach(println)

    // ---------------------------------------------------
    // example 4
//     df = sqlContext.sql( "SELECT COUNT(*) FROM adult WHERE age > 25 AND age < 60" )
//     df.map(t => "Count - " + t(0)).collect().foreach(println)

    // ---------------------------------------------------
    // example 6

    val selectClause = "SELECT COUNT(*) FROM "
    val tableClause = " ( SELECT age,education,occupation FROM adult) t1 " 
    val filterClause = "WHERE ( t1.age > 25 ) "
    val groupClause = ""
    val orderClause = ""

    df = sqlContext.sql( selectClause + tableClause + filterClause +
                                 groupClause + orderClause
                               )

    df.map(t => "Count - " + t(0)).collect().foreach(println)

  } // end main
}