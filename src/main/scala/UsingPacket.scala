import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit, split, when}

import scala.util.Try
import org.apache.spark.sql.DataFrame


object UsingPacket {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark")
      .config("spark.sql.shuffle.partitions", 300)
      .config("spark.worker.cleanup.enabled", "True")
      .config("spark.driver.maxResultSize", "10G")
      .config("spark.local.dir", "/tmp/spark-temp")
      .getOrCreate()

    val hd_conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hd_conf)
    val dirPath = new Path("/user/MobiScore_DataSource/MobiCS_mobile_internet")
    val files = fs.listStatus(dirPath)

    // get phone number
    var df = spark.read.parquet("/user/MobiScore_Output/subscriber.parquet")
    df = df.select("col0").distinct()

    val const_df = df

    val fileName = args(1)
    val configDf = spark.read.option("multiline", "true").json(fileName)
    val START_MONTH = toInt(configDf.groupBy("START_MONTH").mean().collect()(0)(0).toString)
    val YEAR = toInt(configDf.groupBy("YEAR").mean().collect()(0)(0).toString)

    for (i <- START_MONTH to START_MONTH + 5){
      var tmpMonth = i
      if(i>12){
        tmpMonth = i - 12;
      }

      for (file <- files){
        // get month from filePath file.getPath.toString
        val month_real = toInt(file.getPath.toString.slice(105, 105 + 2))
        val year = toInt(file.getPath.toString.slice(101, 101 + 4))
        //filter months
        if (tmpMonth == month_real && year == YEAR){
          println(year)
          println(file.getPath.toString)
          println(month_real)

          // get month from date, total packet
          var df_internet = spark.read.parquet(file.getPath.toString)
          df_internet = df_internet.withColumn("MONTH", expr("substring(_c1, 6, 2)"))
          df_internet = df_internet.filter(df_internet("MONTH") === month_real)

          df_internet.createOrReplaceTempView("table")
          df_internet = spark.sql("select distinct _c0 from table")
          df_internet = df_internet.withColumn("packet_using" , lit(1))

          // check if exist column packet_i or not
          if (hasColumn(df, "packet_" + month_real)) {
            df = df.join(df_internet, df("col0") === df_internet("_c0"), "left")

            df = df.withColumnRenamed("packet_" + month_real, "tmp")

            df = df.withColumn("packet_" + month_real, when(df("tmp") === 1 || df("packet_using") === 1, 1).otherwise(0))

            df = df.select("col0", "packet_" + month_real)
          } else {
            df = df.join(df_internet, df("col0") === df_internet("_c0"), "left")
            df = df.drop("_c0")
            df = df.withColumnRenamed("packet_using", "packet_" + month_real)
          }
        }
      }

      df.show(false)
      df.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/mobile_internet/mobile_internet_dataframe("+tmpMonth+")("+YEAR+").parquet")
      df = const_df
    }

    println("Done")
  }
}
