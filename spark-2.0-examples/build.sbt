name := "spark-2.0-examples"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(

    "org.apache.spark" %% "spark-core" % "2.0.0" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-hive" % "2.0.0" % "provided" withSources() withJavadoc(),
    "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0" % "provided" withSources() withJavadoc()
)

// A special option to exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Configure JAR used with the assembly plug-in
jarName in assembly := "spark2-examples--assembly.jar"
