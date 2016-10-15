name := "spark-2.0-examples"

version := "0.1"

scalaVersion := "2.11.8"

val sparkV = "2.0.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkV % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql"       % sparkV % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-hive"      % sparkV % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming" % sparkV % "provided" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-mllib"     % sparkV % "provided" withSources() withJavadoc(),
  
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3" % "provided" withSources() withJavadoc()
)

// A special option to exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Configure JAR used with the assembly plug-in
jarName in assembly := "spark2-examples--assembly.jar"
