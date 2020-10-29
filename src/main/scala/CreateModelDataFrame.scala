import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SparkSession}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{col, expr, lit, split, when}
import org.apache.hadoop.fs.Path

object CreateModelDataFrame{
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
    val dirPath = new Path("/user/MobiScore_DataSource/MobiCS_debit")
    val files = fs.listStatus(dirPath )

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
    conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
    conf.set("hbase.rootdir","/apps/hbase/data")
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf.set("hbase.cluster.distributed","true")
    new HBaseContext(spark.sparkContext, conf)

    // get phone number
    var score = spark.read.parquet("/user/MobiScore_DataSource/subscriber")
    score = score.withColumn("tmp_score", split(score("_c0"), "\\|"))
    score = score.withColumn("col0", score("tmp_score").getItem(0))
    score = score.select("col0")

    // get real score
    var score_real = spark.read.parquet("/user/MobiScore_DataSource/M_SCORE/FileName=M_SCORE.txt")
    score_real.createOrReplaceTempView("real_score")
    score_real = spark.sql("select _c0, _c12 from (select _c0, _c12 , _c1 , max(_c1) over (partition by _c0) as later_time from real_score) as tmp where _c1 = later_time")
    score_real = score_real.withColumn("label", score_real("_c12")*35/100)

    // get start month
    var flag = 0
    var startMonth = 0

    for (file <- files){
      var df_debit = spark.read.parquet(file.getPath.toString)

      df_debit = df_debit.withColumn("MONTH", expr("substring(_c2, 6, 2)"))
      df_debit = df_debit.drop(df_debit("_c0"))

      val month = toInt(df_debit.select(col("MONTH")).collect()(0)(0).toString)

      if(flag == 0){
        startMonth = month
        flag = 1
      }

      df_debit.createOrReplaceTempView("table")
      var result = spark.sql("select distinct _c1, _c3, _c5 from table")
      result.createOrReplaceTempView("table")
      result = spark.sql("select _c1, sum(_c3) as SUM_USE, sum(_c5) as SUM_PAY_tmp_score from table group by _c1 order by _c1")
      result.createOrReplaceTempView("table")
      result = spark.sql("select *, IFNULL(SUM_PAY_tmp_score, 0) as SUM_PAY from table")
      result = result.drop(result("SUM_PAY_tmp_score"))

      // filter outlier then calculate avg value
      val result_filter = result.filter(result("SUM_USE") < 4000000 || result("SUM_PAY") < 4000000)

      val avg_using = result_filter.agg(expr("avg(SUM_USE)").as("AVG_USE")).select(col("AVG_USE")).collect()(0)(0)
      val avg_paying = result_filter.agg(expr("avg(SUM_PAY)").as("AVG_PAY")).select(col("AVG_PAY")).collect()(0)(0)

      result = result.withColumn("AVG_USING", lit(avg_using))
      result = result.withColumn("AVG_PAYING", lit(avg_paying))

      // determine feature value
      result = result.withColumn("PAY_IN_TIME" , result("SUM_USE") <= result("SUM_PAY"))
      result = result.withColumn("SPARE_PAYMENT" , result("SUM_USE") < result("SUM_PAY"))
      result = result.withColumn("HIGHER_THAN_AVG_USING", result("SUM_USE") >= avg_using)
      result = result.withColumn("HIGHER_THAN_AVG_PAYING", result("SUM_PAY") >= avg_paying)

      // feature: paying date
      df_debit.createOrReplaceTempView("table")
      var tmp_df = spark.sql("select _c1, datediff(_c2 , _c4) AS datediff from table")
      tmp_df.createOrReplaceTempView("table")
      tmp_df = spark.sql("select _c1, max(datediff) as max_date, min(datediff) as min_date, avg(datediff) as avg_date from table")
      tmp_df.na.fill(-180)
      tmp_df = tmp_df.withColumnRenamed("_c1", "id")
      result = result.join(tmp_df, result("_c1") === tmp_df("id"))
      result = result.drop("id")

      //calculate payment history score
      var tmp_score = result.na.drop()
      tmp_score = tmp_score.withColumn("sc_1_"+ month, when(col("PAY_IN_TIME") === true, 1).otherwise(0))
        .withColumn("sc_2_"+ month, when(col("SPARE_PAYMENT") === true, 1).otherwise(0))
        .withColumn("sc_3_"+ month, when(col("HIGHER_THAN_AVG_USING") === true, 1).otherwise(0))
        .withColumn("sc_4_"+ month, when(col("HIGHER_THAN_AVG_PAYING") === true, 1).otherwise(0))
        .withColumnRenamed("max_date", "max_date_" + month)
        .withColumnRenamed("min_date", "min_date_" + month)
        .withColumnRenamed("avg_date", "avg_date_" + month)
        .select("_c1", "sc_1_" + month, "sc_2_"+ month, "sc_3_"+ month, "sc_4_"+ month, "max_date_" + month, "min_date_" + month, "avg_date_" + month)

      score = score.join(tmp_score, score("col0") === tmp_score("_c1"), "left")
    }

    score = score.drop("_c1")
    score.show(false)
    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_model_dataframe.parquet")

    println("Done")
  }
}
