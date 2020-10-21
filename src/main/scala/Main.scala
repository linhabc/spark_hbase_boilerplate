import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{col, expr, lit, split, sum, when}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{IntegerType, StringType}

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
    val dirPath = new Path("/user/MobiScore_DataSource/MobiCS_debit")
    val files = fs.listStatus(dirPath )

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
    conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
    conf.set("hbase.rootdir","/apps/hbase/data")
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf.set("hbase.cluster.distributed","true")
    new HBaseContext(spark.sparkContext, conf)

    // get phome number
    var score = spark.read.parquet("/user/MobiScore_DataSource/subscriber")
    score = score.withColumn("tmp_score", split(score("_c0"), "\\|"))
    score = score.withColumn("col0", score("tmp_score").getItem(0))
    score = score.select("col0")

    for (file <- files){
      var df_debit = spark.read.parquet(file.getPath.toString)

      df_debit = df_debit.withColumn("MONTH", expr("substring(_c2, 6, 2)"))
      df_debit = df_debit.drop(df_debit("_c0"))

      val month = toInt(df_debit.select(col("MONTH")).collect()(0)(0).toString)

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

      //calculate payment history score
      var tmp_score = result.na.fill(false)
      tmp_score = tmp_score.withColumn("sc_1", when(col("PAY_IN_TIME") === true, 45).otherwise(-1))
                          .withColumn("sc_2", when(col("SPARE_PAYMENT") === true, 1).otherwise(0))
                          .withColumn("sc_3", when(col("HIGHER_THAN_AVG_USING") === true, 1).otherwise(0))
                          .withColumn("sc_4", when(col("HIGHER_THAN_AVG_PAYING") === true, 1).otherwise(0))
                          .withColumn("score_" + month, col("sc_1")+col("sc_2")+col("sc_3")+col("sc_4"))
                          .select("_c1", "score_" + month)

      score = score.join(tmp_score, score("col0") === tmp_score("_c1"), "left")
      score = score.withColumn("score_" + month, col("score_" + month).cast(IntegerType))

      // save to database
      result.printSchema()
      result.show(false)

      // type casting
      result = result.withColumn("SUM_USE", col("SUM_USE").cast(IntegerType))
      result = result.withColumn("SUM_PAY", col("SUM_PAY").cast(IntegerType))
      result = result.withColumn("AVG_USING", col("AVG_USING").cast(IntegerType))
      result = result.withColumn("AVG_PAYING", col("AVG_PAYING").cast(IntegerType))
      result = result.withColumn("SPARE_PAYMENT", col("SPARE_PAYMENT").cast(IntegerType))

      result = result.withColumn("SUM_USE", col("SUM_USE").cast(StringType))
      result = result.withColumn("SUM_PAY", col("SUM_PAY").cast(StringType))
      result = result.withColumn("AVG_USING", col("AVG_USING").cast(StringType))
      result = result.withColumn("AVG_PAYING", col("AVG_PAYING").cast(StringType))
      result = result.withColumn("PAY_IN_TIME", col("PAY_IN_TIME").cast(StringType))
      result = result.withColumn("SPARE_PAYMENT", col("SPARE_PAYMENT").cast(StringType))
      result = result.withColumn("HIGHER_THAN_AVG_USING", col("HIGHER_THAN_AVG_USING").cast(StringType))
      result = result.withColumn("HIGHER_THAN_AVG_PAYING", col("HIGHER_THAN_AVG_PAYING").cast(StringType))

//      result.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/post_payment_"+month+".parquet")
//
//      result.write.format("org.apache.hadoop.hbase.spark")
//        .option("hbase.table", configDf.groupBy("TABLE_NAME").mean().collect()(0)(0).toString)
//        .option("hbase.columns.mapping", "_c1 STRING :key , SUM_USE STRING month_%s:SUM_USE_%s, SUM_PAY STRING month_%s:SUM_PAY_%s, PAY_IN_TIME STRING month_%s:PAY_IN_TIME_%s, SPARE_PAYMENT STRING month_%s:SPARE_PAYMENT_%s, AVG_USING STRING month_%s:AVG_USING_%s, AVG_PAYING STRING month_%s:AVG_PAYING_%s, HIGHER_THAN_AVG_USING STRING month_%s:HIGHER_THAN_AVG_USING_%s, HIGHER_THAN_AVG_PAYING STRING month_%s:HIGHER_THAN_AVG_PAYING_%s".format(month, month, month, month, month, month, month, month, month, month, month, month, month, month, month, month))
//        .save()
    }

    score = score.select("col0","score_3","score_4","score_5","score_6","score_7","score_8")
    score = score.na.fill(0)

    score.createOrReplaceTempView("score")
    score = score.withColumn("score_estimate", score("score_3")+score("score_4")+score("score_5")+score("score_6")+score("score_7")+score("score_8"))
    score = score.withColumn("score_estimate", when(score("score_estimate") > 45*6, score("score_estimate") + 2*6).otherwise(score("score_estimate")))
                 .select("col0", "score_estimate")

    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_estimate.parquet")
    println("Done")
  }
}