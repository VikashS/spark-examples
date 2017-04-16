name := "spark-2.0-examples"

version := "0.1"

scalaVersion := "2.11.8"

val sparkV = "2.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkV,
  "org.apache.spark" %% "spark-sql"       % sparkV
)

// A special option to exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Configure JAR used with the assembly plug-in
jarName in assembly := "spark2-examples--assembly.jar"

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @_*) => 
	  (xs map {_.toLowerCase}) match { 
	    case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
	    case _ => MergeStrategy.discard
	  }
	case _ => MergeStrategy.first
}

