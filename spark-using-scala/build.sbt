name := "spark-using-scala"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(

	"org.apache.spark" %% "spark-core" % "1.6.1" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided" withSources() withJavadoc(),
//    "org.scalacheck" %% "scalacheck" % "1.13.0", 
//    "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
//    "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2",

    
    "net.sf.opencsv"   % "opencsv" % "2.3"  withSources() withJavadoc()
)

// A special option to exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Configure JAR used with the assembly plug-in
jarName in assembly := "spark-using-scala-assembly.jar"

// Only include if using assembly
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => 
//    {
//        case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
//        case PathList("org", "apache", xs @ _*) => MergeStrategy.first
//        case "about.html" => MergeStrategy.rename
//        case x => old(x)
//    }
//}

//assemblyMergeStrategy in assembly := {
//	case PathList("META-INF", xs @_*) => 
//	  (xs map {_.toLowerCase}) match { 
//	    case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
//	    case _ => MergeStrategy.discard
//	  }
//	case _ => MergeStrategy.first
//}

//assemblyExcludedJars in assembly := { 
//  val cp = (fullClasspath in assembly).value
//  cp filter {_.data.getName == "cassandra-driver-core-2.1.3-sources.jar"}
//}

//fork in run := true