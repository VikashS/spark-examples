package scala.examples.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object ReadFromCassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ReadFromCassandra")
    conf.set("spark.ui.port", "4141");
    conf.set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)

    val dataRDD = sc.cassandraTable("emju","household_redeemed_offer_display")
    
//    println(dataRDD.count)
    
    dataRDD.take(10).foreach(println(_))
    
    println(dataRDD.first().columnNames)
    
    val cc = new org.apache.spark.sql.cassandra.CassandraSQLContext(sc)
    
    import com.datastax.spark.connector._

    
//    dataRDD.saveAsTextFile("/Users/aahme25/Data_Files/household_redeemed_offer_display_1")
    val p = cc.sql("select * from emju.household_redeemed_offer_display")
    p.take(10).foreach(println(_))
    
  }
}