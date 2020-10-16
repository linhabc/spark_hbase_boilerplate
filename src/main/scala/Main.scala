import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration

object Main{
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)

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

    df = df.select("_c0","_c12")
    df = df.withColumnRenamed("_c0", "ISDN")
           .withColumnRenamed("_c12", "OFFICIAL_SCORE")

    df.printSchema()
    df.show(false)

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table", configDf.groupBy("TABLE_NAME").mean().collect()(0)(0).toString)
      .option("hbase.columns.mapping", configDf.groupBy("TABLE_SCHEMA").mean().collect()(0)(0).toString)
      .save()

    println("Done")
  }
}