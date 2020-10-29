import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{col, expr, lit, split, when}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}


object CreateGBModel{
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

    // get phome number
    //    var score = spark.read.parquet("/user/MobiScore_DataSource/subscriber")
    //    score = score.withColumn("tmp_score", split(score("_c0"), "\\|"))
    //    score = score.withColumn("col0", score("tmp_score").getItem(0))
    //    score = score.select("col0")

    // get real score
    var score_real = spark.read.parquet("/user/MobiScore_DataSource/M_SCORE/FileName=M_SCORE.txt")
    score_real.createOrReplaceTempView("real_score")
    score_real = spark.sql("select _c0, _c12 from (select _c0, _c12 , _c1 , max(_c1) over (partition by _c0) as later_time from real_score) as tmp where _c1 = later_time")
    score_real = score_real.withColumn("label", score_real("_c12")*35/100)

    //     get start month
    //    var flag = 0
    //    var startMonth = 0

    //    for (file <- files){
    //      var df_debit = spark.read.parquet(file.getPath.toString)
    //
    //      df_debit = df_debit.withColumn("MONTH", expr("substring(_c2, 6, 2)"))
    //      df_debit = df_debit.drop(df_debit("_c0"))
    //
    //      val month = toInt(df_debit.select(col("MONTH")).collect()(0)(0).toString)
    //
    //      if(flag == 0){
    //        startMonth = month
    //        flag = 1
    //      }
    //
    //      df_debit.createOrReplaceTempView("table")
    //      var result = spark.sql("select distinct _c1, _c3, _c5 from table")
    //      result.createOrReplaceTempView("table")
    //      result = spark.sql("select _c1, sum(_c3) as SUM_USE, sum(_c5) as SUM_PAY_tmp_score from table group by _c1 order by _c1")
    //      result.createOrReplaceTempView("table")
    //      result = spark.sql("select *, IFNULL(SUM_PAY_tmp_score, 0) as SUM_PAY from table")
    //      result = result.drop(result("SUM_PAY_tmp_score"))
    //
    //      // filter outlier then calculate avg value
    //      val result_filter = result.filter(result("SUM_USE") < 4000000 || result("SUM_PAY") < 4000000)
    //
    //      val avg_using = result_filter.agg(expr("avg(SUM_USE)").as("AVG_USE")).select(col("AVG_USE")).collect()(0)(0)
    //      val avg_paying = result_filter.agg(expr("avg(SUM_PAY)").as("AVG_PAY")).select(col("AVG_PAY")).collect()(0)(0)
    //
    //      result = result.withColumn("AVG_USING", lit(avg_using))
    //      result = result.withColumn("AVG_PAYING", lit(avg_paying))
    //
    //      // determine feature value
    //      result = result.withColumn("PAY_IN_TIME" , result("SUM_USE") <= result("SUM_PAY"))
    //      result = result.withColumn("SPARE_PAYMENT" , result("SUM_USE") < result("SUM_PAY"))
    //      result = result.withColumn("HIGHER_THAN_AVG_USING", result("SUM_USE") >= avg_using)
    //      result = result.withColumn("HIGHER_THAN_AVG_PAYING", result("SUM_PAY") >= avg_paying)
    //
    //      //calculate payment history score
    //      var tmp_score = result.na.drop()
    //      tmp_score = tmp_score.withColumn("sc_1_"+ month, when(col("PAY_IN_TIME") === true, 1).otherwise(0))
    //                          .withColumn("sc_2_"+ month, when(col("SPARE_PAYMENT") === true, 1).otherwise(0))
    //                          .withColumn("sc_3_"+ month, when(col("HIGHER_THAN_AVG_USING") === true, 1).otherwise(0))
    //                          .withColumn("sc_4_"+ month, when(col("HIGHER_THAN_AVG_PAYING") === true, 1).otherwise(0))
    //                          .select("_c1", "sc_1_" + month, "sc_2_"+ month, "sc_3_"+ month, "sc_4_"+ month)
    //
    //      score = score.join(tmp_score, score("col0") === tmp_score("_c1"), "left")
    ////      score = score.withColumn("score_" + month, col("score_" + month).cast(IntegerType))
    //    }
    //
    //    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_model_dataframe.parquet")

    var score = spark.read.parquet("/user/MobiScore_Output/post_payment/score_model_dataframe.parquet")
    val startMonth = 3

    score = score.withColumn("score_in_time", (score("sc_1_"+startMonth)+score("sc_1_"+(startMonth+1))+score("sc_1_"+(startMonth+2))+score("sc_1_"+(startMonth+3))+score("sc_1_"+(startMonth+4))+score("sc_1_"+(startMonth+5)))*SCORE_IN_TIME)
    score = score.withColumn("score_spare_payment", (score("sc_2_"+startMonth)+score("sc_2_"+(startMonth+1))+score("sc_2_"+(startMonth+2))+score("sc_2_"+(startMonth+3))+score("sc_2_"+(startMonth+4))+score("sc_2_"+(startMonth+5)))*SCORE_SPARE_PAYMENT)
    score = score.withColumn("score_avg_using", (score("sc_3_"+startMonth)+score("sc_3_"+(startMonth+1))+score("sc_3_"+(startMonth+2))+score("sc_3_"+(startMonth+3))+score("sc_3_"+(startMonth+4))+score("sc_3_"+(startMonth+5)))*SCORE_AVG_USING)
    score = score.withColumn("score_avg_paying", (score("sc_4_"+startMonth)+score("sc_4_"+(startMonth+1))+score("sc_4_"+(startMonth+2))+score("sc_4_"+(startMonth+3))+score("sc_4_"+(startMonth+4))+score("sc_4_"+(startMonth+5)))*SCORE_AVG_PAYING)

    //    get all feature in here
    score = score.select("col0","score_in_time","score_spare_payment", "score_avg_using", "score_avg_paying")
    score = score.na.drop()
    score = score.withColumn("score_6_month_in_time", when(score("score_in_time") === 6*SCORE_IN_TIME, SCORE_6_MONTH_IN_TIME).otherwise(0))

    score = score.withColumn("score_5_month_in_time", when(score("score_in_time") === 5*SCORE_IN_TIME, SCORE_5_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_4_month_in_time", when(score("score_in_time") === 4*SCORE_IN_TIME, SCORE_4_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_3_month_in_time", when(score("score_in_time") === 3*SCORE_IN_TIME, SCORE_3_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_2_month_in_time", when(score("score_in_time") === 2*SCORE_IN_TIME, SCORE_2_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_1_month_in_time", when(score("score_in_time") === SCORE_IN_TIME, SCORE_1_MONTH_IN_TIME).otherwise(0))

    score = score.join(score_real, score("col0") === score_real("_c0"))
    score = score.drop("_c0")

    // using mlib to get feature score
    val seed = 5043

    val assembler  = new VectorAssembler()
      .setInputCols(Array("score_in_time", "score_spare_payment", "score_avg_using", "score_avg_paying", "score_6_month_in_time", "score_5_month_in_time", "score_4_month_in_time", "score_3_month_in_time", "score_2_month_in_time"))
      .setOutputCol("features")

    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    val Array(pipelineTrainingData, pipelineTestingData) = score.randomSplit(Array(0.7, 0.3), seed)

    val stages = Array(assembler, gbt)

    // build pipeline
    val pipeline = new Pipeline().setStages(stages)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // cross validation
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxBins, Array(25, 28, 31, 32))
      .addGrid(gbt.maxDepth, Array(4, 6, 8))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val cvModel = cv.fit(pipelineTrainingData)

    // test cross validated model with test data
    val cvPredictionDf = cvModel.transform(pipelineTestingData)

    val cvAccuracy = evaluator.evaluate(cvPredictionDf)
    cvModel.write.overwrite()
      .save("/user/MobiScore_Output/post_payment/post_payment_model")

    cvPredictionDf.show(false)
    println(cvAccuracy)
    println("Done")
  }
}
