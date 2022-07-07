import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{col, expr, lit, split, sum, when}
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}

object CreateModelDataFrame{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)

  def main(args: Array [String]){
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark with hbase")
      .config("spark.sql.shuffle.partitions", 300)
      .config("spark.worker.cleanup.enabled", "True")
      .config("spark.driver.maxResultSize", "10G")
      .config("spark.local.dir", "/tmp/spark-temp")
      .getOrCreate()

    val fileName = args(1)
    val configDf = spark.read.option("multiline", "true").json(fileName)
    val startMonth = toInt(configDf.groupBy("START_MONTH").mean().collect()(0)(0).toString)
    var YEAR = toInt(configDf.groupBy("YEAR").mean().collect()(0)(0).toString)

    val hd_conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hd_conf)
    val dirPath = new Path("/user/MobiScore_DataSource/MobiCS_debit")
    val files = fs.listStatus(dirPath)

    // get phone number
    var score = spark.read.parquet("/user/MobiScore_Output/subscriber.parquet")
    score = score.select("col0")

    var flag = false
    for (i <- startMonth to startMonth + 5){
      var tmpMonth = i
      if(i > 12){
        tmpMonth = i - 12;
        if (flag == false) {
          YEAR = YEAR + 1;
          flag = true;
        }
      }

      println("tmpMonth ", tmpMonth)

      for (file <- files){
        val month = toInt(file.getPath.toString.slice(85, 85 + 2))
        val year = toInt(file.getPath.toString.slice(81, 81 + 4))
        println("init month ", month)
        println("init year ", year)

        var debitMonth = month + 1
        var debitYear = year
        if(debitMonth > 12){
          debitMonth = debitMonth - 12;
          debitYear = debitYear + 1;
        }

        println("init debitMonth ", debitMonth, tmpMonth)
        println("init debitYear ", debitYear, YEAR)

        if( debitMonth == tmpMonth  && debitYear == YEAR) {
//          println("startMonth "+startMonth)
//
//          println("month ", month)
//          println("year ", year)
//          println("debitMonth ", debitMonth)
//          println("debitYear ", debitYear)
          println(file.getPath.toString)

          var df_debit = spark.read.parquet(file.getPath.toString)

          df_debit = df_debit.drop(df_debit("_c0"))

          df_debit.count()
//            df_debit.show(false)

          // read using packet dataframe
          val packet_df = spark.read.parquet("/user/MobiScore_Output/post_payment/mobile_internet/mobile_internet_dataframe("+tmpMonth+")("+YEAR+").parquet")

          packet_df.count()
          packet_df.show(false)

          df_debit.createOrReplaceTempView("table")
          var result = spark.sql("select distinct _c1, _c3, _c5, _c6 from table")

          result = result.na.fill(0)
          result = result.na.fill("0")

//            result.show(false)

          result.createOrReplaceTempView("table")
          result = spark.sql("select _c1, sum(_c3) as SUM_USE, sum(_c5) as SUM_PAY, max(_c6) as USING_BANK_tmp from table group by _c1")
//            result.show(false)

          // filter outlier then calculate avg value
          val result_filter = result.filter(result("SUM_USE") < 4000000 || result("SUM_PAY") < 4000000)
          val avg_using = result_filter.agg(expr("avg(SUM_USE)").as("AVG_USE")).select(col("AVG_USE")).collect()(0)(0)
          val avg_paying = result_filter.agg(expr("avg(SUM_PAY)").as("AVG_PAY")).select(col("AVG_PAY")).collect()(0)(0)

          // join with packet_df
          result = result.join(packet_df, result("_c1") === packet_df("col0"), "left")
          result = result.drop("col0")

          result = result.na.fill(0)
          result = result.na.fill("0")

          // determine feature value
          result = result.withColumn("PAY_IN_TIME" , result("SUM_USE") <= result("SUM_PAY"))
          result = result.withColumn("SPARE_PAYMENT" , result("SUM_USE") < result("SUM_PAY"))
          result = result.withColumn("HIGHER_THAN_AVG_USING", result("SUM_USE") >= avg_using)
          result = result.withColumn("HIGHER_THAN_AVG_PAYING", result("SUM_PAY") >= avg_paying)
          result = result.withColumn("USING_BANK", result("USING_BANK_tmp") === 2)
          result = result.withColumn("NOT_USING_PACKET", result("HIGHER_THAN_AVG_PAYING") && result("packet_"+tmpMonth) === 0)

//            result.show(false)

          //calculate payment history score
          result = result.withColumn("sc_1_"+ tmpMonth, when(col("PAY_IN_TIME") === true, 1).otherwise(0))
            .withColumn("sc_2_"+ tmpMonth, when(col("SPARE_PAYMENT") === true, 1).otherwise(0))
            .withColumn("sc_3_"+ tmpMonth, when(col("HIGHER_THAN_AVG_USING") === true && col("PAY_IN_TIME") === true, 1).otherwise(0))
            .withColumn("sc_4_"+ tmpMonth, when(col("HIGHER_THAN_AVG_PAYING") === true, 1).otherwise(0))
            .withColumn("sc_5_"+ tmpMonth, when(col("USING_BANK") === true, 1).otherwise(0))
            .withColumn("sc_6_"+ tmpMonth, when(col("NOT_USING_PACKET") === true, 1).otherwise(0))

          result = result.select("_c1", "sc_1_" + tmpMonth, "sc_2_"+ tmpMonth, "sc_3_"+ tmpMonth, "sc_4_"+ tmpMonth, "sc_5_"+ tmpMonth, "sc_6_"+ tmpMonth)
          //      result = result.select("_c1", "sc_1_" + month, "sc_2_"+ month, "sc_3_"+ month, "sc_4_"+ month, "sc_5_"+ month)

          result.show(false)

          score = score.join(result, score("col0") === result("_c1"), "left")
        }
      }

    }

    score = score.drop("_c1")
    score = score.na.drop()

    score.show(false)
    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_model_dataframe.parquet")

    println("Done")
  }
}