import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.split
import scala.io.Source

object Main{
  def main(args: Array [String]){

    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

  val fileName = args(1)
  val configDf = spark.read.option("multiline", "true").json(fileName)

   val conf = HBaseConfiguration.create()
   conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
   conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
   conf.set("hbase.rootdir","/apps/hbase/data")
   conf.set("zookeeper.znode.parent","/hbase-unsecure")
   conf.set("hbase.cluster.distributed","true")

   new HBaseContext(spark.sparkContext, conf)

   var df = spark.read.parquet(configDf.groupBy("FILE_PATH").mean().collect()(0)(0).toString)

    df = df.withColumn("tmp", split(df("_c0"), "\\|"))
    df = df.withColumn("id", df("tmp").getItem(0))
          .withColumn("col1", df("tmp").getItem(1))
          .withColumn("col2", df("tmp").getItem(2))
          .withColumn("col3", df("tmp").getItem(3))
          .withColumn("col4", df("tmp").getItem(4))
          .withColumn("col5", df("tmp").getItem(5))
          .withColumn("col6", df("tmp").getItem(6))
          .withColumn("col7", df("tmp").getItem(7))

    df = df.drop(df("_c0"))
    df = df.drop(df("tmp"))
    df = df.drop(df("FileName"))

    df.printSchema()

    df.show(30)

   df.write.format("org.apache.hadoop.hbase.spark")
     .option("hbase.table", configDf.groupBy("TABLE_NAME").mean().collect()(0)(0).toString)
     .option("hbase.columns.mapping", configDf.groupBy("TABLE_SCHEMA").mean().collect()(0)(0).toString)
     .save()

   println("Done")
  }
}