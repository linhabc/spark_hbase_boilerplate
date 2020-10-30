import org.apache.spark.sql.{SparkSession}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{when}

object MyPredicted{
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

    score = score.withColumn("score_estimate", score("score_in_time")+score("score_spare_payment")+score("score_avg_using")+score("score_avg_paying")+score("score_6_month_in_time")+score("score_5_month_in_time")+score("score_4_month_in_time")+score("score_3_month_in_time")+score("score_2_month_in_time")+score("score_1_month_in_time"))
    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_estimate_drop.parquet")
    println("Done")
  }
}
