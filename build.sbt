name := "spark_hbase"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.6" % "provided"
libraryDependencies += "org.apache.hbase.connectors.spark" % "hbase-spark" % "1.0.0"

assemblyJarName in assembly := "spark_hbase.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}