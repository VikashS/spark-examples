package scala.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object Ex07_UsingHive2 {
  def main(args: Array[String]): Unit = {

    // create  a spark conf and context
    val conf = new SparkConf().setMaster("local").setAppName("Ex07_UsingHive2")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._
    import hiveContext.sql

    // run some sql

    // -----------------------------------------------------------
    // example 1

        hiveContext.sql( """                                
                                                         
            CREATE TABLE IF NOT EXISTS adult2
               (                                          
                 idx             INT,                     
                 age             INT,                     
                 workclass       STRING,                    
                 fnlwgt          INT,                     
                 education       STRING,                    
                 educationnum    INT,                     
                 maritalstatus   STRING,                    
                 occupation      STRING,                    
                 relationship    STRING,                    
                 race            STRING,                    
                 gender          STRING,                    
                 capitalgain     INT,                     
                 capitalloss     INT,                     
                 nativecountry   STRING,                    
                 income          STRING                    
               )                    
                        
                        """)

        var resRDD = hiveContext.sql("SELECT COUNT(*) FROM adult2")
        resRDD.map(t => "Count : " + t(0) ).collect().foreach(println)

    // -----------------------------------------------------------
    // example 2

    hiveContext.sql("""

        CREATE EXTERNAL TABLE IF NOT EXISTS adult3
           (                                          
             idx             INT,                     
             age             INT,                     
             workclass       STRING,                    
             fnlwgt          INT,                     
             education       STRING,                    
             educationnum    INT,                     
             maritalstatus   STRING,                    
             occupation      STRING,                    
             relationship    STRING,                    
             race            STRING,                    
             gender          STRING,                    
             capitalgain     INT,                     
             capitalloss     INT,                     
             nativecountry   STRING,                    
             income          STRING                    
           )                    
           ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' 
           LOCATION 'file:///Users/aahme25/GitHub/spark-examples/spark-using-scala/data/hive'

                   """)

    // take another count 
     resRDD = hiveContext.sql("SELECT COUNT(*) FROM adult3")
     resRDD.map(t => "Count : " + t(0) ).collect().foreach(println)

    // -----------------------------------------------------------
    // example 3

     resRDD = hiveContext.sql("""

          SELECT t1.edu FROM
          ( SELECT DISTINCT education AS edu FROM adult3 ) t1
          ORDER BY t1.edu 

                    """)

     resRDD.map(t => t(0) ).collect().foreach(println)

    // -----------------------------------------------------------
    // example 4

    // drop and recreate education table

//        hiveContext.sql("""
//    
//           DROP TABLE IF EXISTS education
//    
//                       """)
//
//        hiveContext.sql("""
//    
//          CREATE TABLE IF NOT EXISTS  education 
//            (
//              idx        INT,                     
//              name       STRING                    
//            )
//            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
//            LOCATION '/data/spark/dim1/'
//    
//                       """)
//    
//    
//        // output result
//    
//        val resRDD = hiveContext.sql("""
//    
//           SELECT * FROM education
//    
//                       """)
//    
//        resRDD.map( t => t(0)+" "+t(1) ).collect().foreach(println)

  }
}
//export CLASSPATH="$CLASSPATH:/usr/local/java/spark-1.6.1-bin-hadoop2.6/lib/mysql-connector-java-5.1.35-bin.jar"
//export SPARK_CLASSPATH=$CLASSPATH
//spark-submit --class scala.examples.sql.Ex07_UsingHive2 --master spark://`hostname`:7077 target/scala-2.10/spark-using-scala-assembly.jar