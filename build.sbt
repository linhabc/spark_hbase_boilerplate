name := "spark_hbase"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided"
libraryDependencies += "org.apache.hbase.connectors.spark" % "hbase-spark" % "1.0.0"

//libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.2.3.1.0.6-1"
//libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.2.3.1.0.6-1"
//libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"

assemblyJarName in assembly := "spark_hbase.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}