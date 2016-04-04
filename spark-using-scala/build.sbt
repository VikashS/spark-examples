name := "spark-using-scala"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(

	"org.apache.spark" %% "spark-core" % "1.6.1" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided" withSources() withJavadoc(),
    "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided" withSources() withJavadoc(),
    "net.sf.opencsv"   % "opencsv" % "2.3"  withSources() withJavadoc()
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Only include if using assembly
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => 
    {
        case PathList("javax", "servlet", xs @ _*) =>
        MergeStrategy.first
        case PathList("org", "apache", xs @ _*) => MergeStrategy.first
        case "about.html" => MergeStrategy.rename
        case x => old(x)
    }
}

fork in run := true
