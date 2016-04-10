package scala.examples.sql

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object Ex07_UsingHive1 {
  def main(args: Array[String]): Unit = {

    // create  a spark conf and context
    val conf = new SparkConf().setMaster("local").setAppName("Ex07_UsingHive1")
    conf.set("spark.ui.port", "4141");

    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)

    hc.sql("create table if not exists person(first_name string, last_name string, age int) row format delimited fields terminated by ','")

    hc.sql("load data local inpath \"data/hive/person\" into table person")

    //Alternatively, load that data in the person table from HDFS
    //hc.sql("load data inpath \"/user/hduser/person\" into table person")

    val persons = hc.sql("from person select first_name,last_name,age")

    hc.sql("create table if not exists person2 as select first_name, last_name from person")

    //    hc.sql("create table person2 like person location '/user/hive/warehouse/person'")

    hc.sql("create table if not exists people_by_last_name(last_name string,count int)")

    hc.sql("create table if not exists people_by_age(age int,count int)")

    hc.sql("""from person
                insert overwrite table people_by_last_name
                  select last_name, count(distinct first_name) group by last_name
                insert overwrite table people_by_age
                  select age, count(distinct first_name) group by age """)

    hc.sql("show tables").foreach { println(_) }                  
  }
}
