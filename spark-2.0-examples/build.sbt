name := "spark-2.0-examples"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.2"

libraryDependencies ++= Seq(
	"org.apache.spark"             %%  "spark-core"                       % sparkVersion % "provided",
	"org.apache.spark"             %%  "spark-sql"                        % sparkVersion % "provided",
	"org.apache.spark"             %%  "spark-hive"                       % sparkVersion % "provided",
	"org.apache.spark"             %%  "spark-streaming"                  % sparkVersion % "provided",
    "org.apache.spark"              %  "spark-sql-kafka-0-10_2.11"        % sparkVersion
)

// A special option to exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Configure JAR used with the assembly plug-in
jarName in assembly := "spark2-examples-assembly.jar"

// Only include if using assembly
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => 
//    {
//        case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
//        case PathList("org", "apache", xs @ _*) => MergeStrategy.first
//        case "about.html" => MergeStrategy.rename
//        case x => old(x)
//    }
//}

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @_*) => 
	  (xs map {_.toLowerCase}) match { 
	    case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
	    case _ => MergeStrategy.discard
	  }
	case _ => MergeStrategy.first
}

//assemblyExcludedJars in assembly := { 
//  val cp = (fullClasspath in assembly).value
//  cp filter {_.data.getName == "cassandra-driver-core-2.1.3-sources.jar"}
//}

//fork in run := true