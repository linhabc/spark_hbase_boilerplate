import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, expr, split, when, max, min}

object Main{
  Logger.getLogger("org").setLevel(Level.ERROR)
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

    val hd_conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hd_conf)
    var dirPath = new Path("/user/MobiScore_DataSource/MobiCS_revenue")
    var files = fs.listStatus(dirPath)

    var df_revenue_total = spark.emptyDataFrame

    val START_MONTH = toInt(configDf.groupBy("START_MONTH").mean().collect()(0)(0).toString)
    val YEAR = toInt(configDf.groupBy("YEAR").mean().collect()(0)(0).toString)

    val MONTH_GAP = 2

    for (i <- START_MONTH to START_MONTH + MONTH_GAP){
      for (file <- files) {
        val month_real = toInt(file.getPath.toString.slice(89, 89 + 2))
        val year = toInt(file.getPath.toString.slice(85, 85 + 4))

        if(i == month_real && year == YEAR){
          val rev_mix_month = "REV_MIX_"+month_real

          val df_rev = spark.read.parquet(file.getPath.toString)
            .withColumn(rev_mix_month, when(col("_c3") > 0,1)
              .otherwise(0) + when(col("_c4") > 0,1)
              .otherwise(0) + when(col("_c5") > 0,1)
              .otherwise(0) + when(col("_c6") > 0,1)
              .otherwise(0))
            .withColumnRenamed("_c2", "rev_value_"+month_real)
            .drop("_c1", "_c3", "_c4", "_c5", "_c6")

          if(df_revenue_total == spark.emptyDataFrame) {
            df_revenue_total = df_rev.withColumnRenamed("_c0", "new_id")
                                     .withColumn("CREDIT_MIX", df_rev(rev_mix_month))
          } else {
            df_revenue_total = df_revenue_total.join(df_rev, df_revenue_total("new_id") === df_rev("_c0"))
              .withColumn("CREDIT_MIX", (df_revenue_total("CREDIT_MIX") + df_rev(rev_mix_month)))
              .drop("_c0", "col1", "col2")
          }

          df_revenue_total.show(false)
        }
      }
    }

    df_revenue_total.show(false)

    var df_rev_score = df_revenue_total.withColumn("SCORE_CREDIT_MIX", df_revenue_total("CREDIT_MIX") * 2 * 4 + (110-24*4))

    df_rev_score = df_rev_score.na.fill(0, Array("SCORE_CREDIT_MIX"))
    df_rev_score.show(false)

    df_rev_score.agg(max("SCORE_CREDIT_MIX"), min("SCORE_CREDIT_MIX")).show()

    df_rev_score.write.mode("overwrite").parquet("/user/MobiScore_Output/credit_mix_2")

   println("Done")
  }
}