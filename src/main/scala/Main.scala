import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, current_date, expr, lit, max, min, months_between, split, when}

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
    var dirPath = new Path("/user/MobiScore_DataSource/FINTECH_LOYALTY")
    var files = fs.listStatus(dirPath)

    var df_loyalty_total = spark.emptyDataFrame

    val START_MONTH = toInt(configDf.groupBy("START_MONTH").mean().collect()(0)(0).toString)
    val YEAR = toInt(configDf.groupBy("YEAR").mean().collect()(0)(0).toString)

    for (i <- START_MONTH to START_MONTH + 3){
      for (file <- files) {
        val month_real = toInt(file.getPath.toString.slice(117, 117 + 2))
        val year = toInt(file.getPath.toString.slice(112, 112 + 4))

        if(i == month_real && year == YEAR){

          val df_loyalty = spark.read.parquet(file.getPath.toString)

          // tach cot va gan nhan loyalty
          val dfc = df_loyalty.withColumn("_c0", split(col("_c0"), ",")).select(
            col("*") +: (0 until 2).map(
              i => col("_c0").getItem(i).as(s"col$i")
            ): _*
          ).withColumn("col2", when(col("col1") === "BRONZE", 1)
            .when(col("col1") === "SILVER",2)
            .when(col("col1") === "TITAN",3)
            .when(col("col1") === "GOLD",4)
            .when(col("col1") === "DIAMOND",5)
            .otherwise(0)
          ).drop("_c0")

          if(df_loyalty_total == spark.emptyDataFrame) {
            df_loyalty_total = dfc.withColumnRenamed("col0", "new_id")
                                  .withColumnRenamed("col2", "LOYALTY_VAL")
                                  .drop("col1")
          }
          df_loyalty_total = df_loyalty_total.join(dfc, df_loyalty_total("new_id") === dfc("col0"))
            .withColumn("LOYALTY_VAL", expr("(LOYALTY_VAL + col2)/2")).drop("col0", "col1", "col2")
        }
      }
    }

    df_loyalty_total.show(false)

    dirPath = new Path("/user/MobiScore_DataSource/VLR_MONTHLY")
    files = fs.listStatus(dirPath)
    var vlr_monthly_total = spark.emptyDataFrame
    for (i <- START_MONTH to START_MONTH + 3){
      for (file <- files) {
        val month_real = toInt(file.getPath.toString.slice(90, 90 + 2))
        val year = toInt(file.getPath.toString.slice(86, 86 + 4))

        if(i == month_real && year == YEAR){

          val vlr_monthly = spark.read.parquet(file.getPath.toString)
                                  .withColumn("vlr", (col("_c1")/31*100).cast("Integer"))
                                  .drop("_c1")

          if(vlr_monthly_total == spark.emptyDataFrame) {
            vlr_monthly_total = vlr_monthly.withColumnRenamed("_c0", "new_id")
                                            .withColumnRenamed("vlr", "VLR_AVAIL")
          }

          vlr_monthly_total = vlr_monthly_total.join(vlr_monthly, vlr_monthly_total("new_id") === vlr_monthly("_c0"), "left")
                                              .withColumn("VLR_AVAIL", expr("(VLR_AVAIL + vlr)/2"))
                                              .drop("_c0").drop("vlr")
        }
      }
    }

    vlr_monthly_total.show(false)

    // sub_los
    val df_los = spark.read.parquet("/user/MobiScore_DataSource/MobiCS_subscriber/FileName=subscriber_202107.txt")
    val dfc_los = df_los.withColumn("LOS",  months_between(current_date(),col("_c1")).cast("Integer"))
                        .drop("_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7")
    dfc_los.show(false)

    // sodep
    val df_sodep = spark.read.parquet("/user/MobiScore_DataSource/SODEP/FileName=SODEP.txt")
                        .withColumn("SODEP", lit(1)).withColumnRenamed("_c0", "new_id")

    // join loyalty & sub_los
    val dfc_credit_history = dfc_los.join(df_loyalty_total, dfc_los("_c0") ===  df_loyalty_total("new_id"), "left")
                                    .join(df_sodep, dfc_los("_c0") === df_sodep("new_id"), "left")
                                    .join(vlr_monthly_total, dfc_los("_c0") === vlr_monthly_total("new_id"), "left")
                                    .drop("new_id")

    dfc_credit_history.show(false)

    val dfc_credit_his_score = dfc_credit_history.withColumn("LOS_SCORE",
      when(col("LOS") < 3, 0)
        .when(col("LOS") < 12, 14)
        .when(col("LOS") < 24, 34)
        .when(col("LOS") < 36, 40)
        .when(col("LOS") < 54, 48)
        .when(col("LOS") < 80, 56)
        .when(col("LOS") >= 80, 68)  // max 68
        .otherwise(0)
    ).withColumn("SODEP_SCORE",
      when(col("SODEP") === 1, 30)  // max 30
        .otherwise(0)
    ).withColumn("LOYALTY_SCORE",
      when(col("LOYALTY_VAL") >= 1, (col("LOYALTY_VAL") + 1) * 5)   // max: 30
        .otherwise(3)
    ).withColumn("VLR_SCORE",
      when(col("VLR_AVAIL") >= 1, (col("VLR_AVAIL")/10).cast("Integer")*2)  // max: 18
        .otherwise(0)
    ).withColumn("LENGTH_CREDIT_HIS_SCORE", expr("LOS_SCORE + SODEP_SCORE + LOYALTY_SCORE + VLR_SCORE"))

    dfc_credit_his_score.show(false)

    dfc_credit_his_score.agg(max("LENGTH_CREDIT_HIS_SCORE"), min("LENGTH_CREDIT_HIS_SCORE")).show()

    dfc_credit_his_score.write.mode("overwrite").parquet("/user/MobiScore_Output/length_credit_his_2/")
    println("Done")
  }
}