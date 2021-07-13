import MyPredicted.toInt
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit, split, when}
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}

object GetUserSubscribe{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)

  def main(args: Array [String]){
    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

    val hd_conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hd_conf)
    val dirPath = new Path("/user/MobiScore_DataSource/MobiCS_subscriber")
    val files = fs.listStatus(dirPath )

    var user = spark.emptyDataFrame
    var flag = 0

    val fileName = args(1)
    val configDf = spark.read.option("multiline", "true").json(fileName)
    val START_MONTH = toInt(configDf.groupBy("START_MONTH").mean().collect()(0)(0).toString)
    val YEAR = toInt(configDf.groupBy("YEAR").mean().collect()(0)(0).toString)

    for (file <- files){
      var df_debit = spark.read.parquet(file.getPath.toString)

//      df_debit = df_debit.withColumn("MONTH", expr("substring(_c2, 6, 2)"))
//      df_debit = df_debit.drop(df_debit("_c0"))

//      val month = toInt(df_debit.select(col("MONTH")).collect()(0)(0).toString)

      val month = toInt(file.getPath.toString.slice(95, 95 + 2))

      val year = toInt(file.getPath.toString.slice(91, 91 + 4))


      if(START_MONTH <= month && month <= START_MONTH + 5 && year == YEAR) {
        println(year)
        println(file.getPath.toString)
        println(month)

        df_debit.createOrReplaceTempView("table")
        val user_in_month = spark.sql("select distinct _c0 as col0 from table")

        if (flag == 0) {
          user = user_in_month
          flag = 1
        }

        user = user.intersect(user_in_month)
      }
    }

    user.show(false)
    user.write.mode("overwrite").parquet("/user/MobiScore_Output/subscriber.parquet")
    println("Done")
  }
}