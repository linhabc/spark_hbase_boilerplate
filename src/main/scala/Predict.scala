import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{col, expr, lit, split, when}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.tuning.CrossValidatorModel

object Predict{
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

    val SCORE_IN_TIME = toInt(configDf.groupBy("SCORE_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_SPARE_PAYMENT = toInt(configDf.groupBy("SCORE_SPARE_PAYMENT").mean().collect()(0)(0).toString)
    val SCORE_AVG_USING = toInt(configDf.groupBy("SCORE_AVG_USING").mean().collect()(0)(0).toString)
    val SCORE_AVG_PAYING = toInt(configDf.groupBy("SCORE_AVG_PAYING").mean().collect()(0)(0).toString)
    val SCORE_6_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_6_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_5_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_5_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_4_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_4_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_3_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_3_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_2_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_2_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_1_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_1_MONTH_IN_TIME").mean().collect()(0)(0).toString)

    // load random forest model
    val cvModelLoaded = CrossValidatorModel
      .load("/user/MobiScore_Output/post_payment/post_payment_model")

    // get phome number
    var score = spark.read.parquet("/user/MobiScore_DataSource/subscriber")
    score = score.withColumn("tmp_score", split(score("_c0"), "\\|"))
    score = score.withColumn("col0", score("tmp_score").getItem(0))
    score = score.select("col0")

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
      tmp_df = spark.sql("select _c1, max(datediff) as max_date, min(datediff) as min_date, avg(datediff) as avg_date from table group by _c1")
      tmp_df.na.fill(-180)
      tmp_df = tmp_df.withColumnRenamed("_c1", "id")
      result = result.join(tmp_df, result("_c1") === tmp_df("id"))
      result = result.withColumn("max_date", when(result("SPARE_PAYMENT") === true, 1).otherwise(result("max_date")))
      result = result.withColumn("min_date", when(result("SPARE_PAYMENT") === true, 1).otherwise(result("min_date")))
      result = result.withColumn("avg_date", when(result("SPARE_PAYMENT") === true, 1).otherwise(result("avg_date")))
      result = result.drop("id")

      //calculate payment history score
      var tmp_score = result.na.drop()
      tmp_score = tmp_score.withColumn("sc_1_"+ month, when(col("PAY_IN_TIME") === true, 1).otherwise(0))
        .withColumn("sc_2_"+ month, when(col("SPARE_PAYMENT") === true, 1).otherwise(0))
        .withColumn("sc_3_"+ month, when(col("HIGHER_THAN_AVG_USING") === true, 1).otherwise(0))
        .withColumn("sc_4_"+ month, when(col("HIGHER_THAN_AVG_PAYING") === true, 1).otherwise(0))
//        .withColumnRenamed("max_date", "max_date_" + month)
//        .withColumnRenamed("min_date", "min_date_" + month)
        .withColumnRenamed("avg_date", "avg_date_" + month)
        .select("_c1", "sc_1_" + month, "sc_2_"+ month, "sc_3_"+ month, "sc_4_"+ month, "max_date_" + month, "min_date_" + month, "avg_date_" + month)

      score = score.join(tmp_score, score("col0") === tmp_score("_c1"), "left")
      //      score = score.withColumn("score_" + month, col("score_" + month).cast(IntegerType))
    }

    score = score.withColumn("score_in_time", (score("sc_1_"+startMonth)+score("sc_1_"+(startMonth+1))+score("sc_1_"+(startMonth+2))+score("sc_1_"+(startMonth+3))+score("sc_1_"+(startMonth+4))+score("sc_1_"+(startMonth+5)))*SCORE_IN_TIME)
    score = score.withColumn("score_spare_payment", (score("sc_2_"+startMonth)+score("sc_2_"+(startMonth+1))+score("sc_2_"+(startMonth+2))+score("sc_2_"+(startMonth+3))+score("sc_2_"+(startMonth+4))+score("sc_2_"+(startMonth+5)))*SCORE_SPARE_PAYMENT)
    score = score.withColumn("score_avg_using", (score("sc_3_"+startMonth)+score("sc_3_"+(startMonth+1))+score("sc_3_"+(startMonth+2))+score("sc_3_"+(startMonth+3))+score("sc_3_"+(startMonth+4))+score("sc_3_"+(startMonth+5)))*SCORE_AVG_USING)
    score = score.withColumn("score_avg_paying", (score("sc_4_"+startMonth)+score("sc_4_"+(startMonth+1))+score("sc_4_"+(startMonth+2))+score("sc_4_"+(startMonth+3))+score("sc_4_"+(startMonth+4))+score("sc_4_"+(startMonth+5)))*SCORE_AVG_PAYING)

//    score = score.withColumn("score_max_date", (score("max_date_"+startMonth)+score("max_date_"+(startMonth+1))+score("max_date_"+(startMonth+2))+score("max_date_"+(startMonth+3))+score("max_date_"+(startMonth+4))+score("max_date_"+(startMonth+5)))/6)
//    score = score.withColumn("score_min_date", (score("min_date_"+startMonth)+score("min_date_"+(startMonth+1))+score("min_date_"+(startMonth+2))+score("min_date_"+(startMonth+3))+score("min_date_"+(startMonth+4))+score("min_date_"+(startMonth+5)))/6)
    score = score.withColumn("score_avg_date", (score("avg_date_"+startMonth)+score("avg_date_"+(startMonth+1))+score("avg_date_"+(startMonth+2))+score("avg_date_"+(startMonth+3))+score("avg_date_"+(startMonth+4))+score("avg_date_"+(startMonth+5)))/6)

    // get all feature in here
    score = score.select("col0","score_in_time","score_spare_payment", "score_avg_using", "score_avg_paying",  "score_avg_date")
    score = score.na.drop()
    score = score.withColumn("score_6_month_in_time", when(score("score_in_time") === 6*SCORE_IN_TIME, SCORE_6_MONTH_IN_TIME).otherwise(0))

    score = score.withColumn("score_5_month_in_time", when(score("score_in_time") === 5*SCORE_IN_TIME, SCORE_5_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_4_month_in_time", when(score("score_in_time") === 4*SCORE_IN_TIME, SCORE_4_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_3_month_in_time", when(score("score_in_time") === 3*SCORE_IN_TIME, SCORE_3_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_2_month_in_time", when(score("score_in_time") === 2*SCORE_IN_TIME, SCORE_2_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_1_month_in_time", when(score("score_in_time") === SCORE_IN_TIME, SCORE_1_MONTH_IN_TIME).otherwise(0))

    // predict using model
    score = cvModelLoaded.transform(score)

    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_model_predict.parquet")

    score.createOrReplaceTempView("score_predict")

    val real_score = spark.read.parquet("/user/MobiScore_DataSource/M_SCORE/FileName=M_SCORE.txt")
    real_score.createOrReplaceTempView("real_score")

    val result = spark.sql("select count(*) from (select r._c0, _c12, s.prediction, (prediction/_c12*100) ratio from score_predict s, real_score r where s.col0 = r._c0 and s.prediction > 0) tmp where tmp.ratio >= 25 and tmp.ratio < 38")
    result.show(false)
    println("Done")
  }
}
