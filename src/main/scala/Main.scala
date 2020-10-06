import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object Main{
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)

  def main(args: Array [String]){

    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

  val fileName = args(1)
  val configDf = spark.read.option("multiline", "true").json(fileName)

  val hd_conf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hd_conf)
  val dirPath = new Path(configDf.groupBy("FILE_PATH").mean().collect()(0)(0).toString)
  var files = fs.listStatus(dirPath )

  files = files.sortWith((s, t) => {
     s.getPath.toString.compareTo(t.getPath.toString) > 0
    }
  )

  val latest_file = files(0).getPath.toString
  val near_latest_file = files(1).getPath.toString

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
  conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
  conf.set("hbase.rootdir","/apps/hbase/data")
  conf.set("zookeeper.znode.parent","/hbase-unsecure")
  conf.set("hbase.cluster.distributed","true")

  new HBaseContext(spark.sparkContext, conf)

  var df = spark.read.parquet(near_latest_file)

  df = df.select("_c0","_c1", "_c2")
  df = df.withColumnRenamed("_c0", "old_id")
  df = df.withColumnRenamed("_c1", "BRAND_OLD")
  df = df.withColumnRenamed("_c2", "MODEL_OLD")

  var df_1 = spark.read.parquet(latest_file)

  df_1 = df_1.select("_c0","_c1", "_c2")
  df_1 = df_1.withColumnRenamed("_c0", "new_id")
  df_1 = df_1.withColumnRenamed("_c1", "BRAND_NEW")
  df_1 = df_1.withColumnRenamed("_c2", "MODEL_NEW")

  df_1 = df_1.join(df, df("old_id") === df_1("new_id"))
  df_1 = df_1.withColumn("CHANGED_PHONE", expr("MODEL_NEW != MODEL_OLD"))

  df_1.printSchema()

  df_1 = df_1.drop("old_id")
  df_1.show(10)

  df_1.write.format("org.apache.hadoop.hbase.spark")
    .option("hbase.table", configDf.groupBy("TABLE_NAME").mean().collect()(0)(0).toString)
    .option("hbase.columns.mapping", configDf.groupBy("TABLE_SCHEMA").mean().collect()(0)(0).toString)
    .save()

   println("Done")
  }
}