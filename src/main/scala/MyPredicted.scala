import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{lit, when}

object MyPredicted{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)

  def main(args: Array [String]){
    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

    val fileName = args(1)
    val configDf = spark.read.option("multiline", "true").json(fileName)

//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
//    conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
//    conf.set("hbase.rootdir","/apps/hbase/data")
//    conf.set("zookeeper.znode.parent","/hbase-unsecure")
//    conf.set("hbase.cluster.distributed","true")
//    new HBaseContext(spark.sparkContext, conf)

    val startMonth = toInt(configDf.groupBy("START_MONTH").mean().collect()(0)(0).toString)

    val SCORE_IN_TIME = toInt(configDf.groupBy("SCORE_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_SPARE_PAYMENT = toInt(configDf.groupBy("SCORE_SPARE_PAYMENT").mean().collect()(0)(0).toString)
    val SCORE_AVG_USING = toInt(configDf.groupBy("SCORE_AVG_USING").mean().collect()(0)(0).toString)
    val SCORE_AVG_PAYING = toInt(configDf.groupBy("SCORE_AVG_PAYING").mean().collect()(0)(0).toString)
    val SCORE_USING_BANK = toInt(configDf.groupBy("SCORE_USING_BANK").mean().collect()(0)(0).toString)
    val SCORE_NOT_USING_PACKET = toInt(configDf.groupBy("SCORE_NOT_USING_PACKET").mean().collect()(0)(0).toString)

    val SCORE_6_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_6_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_5_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_5_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_4_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_4_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_3_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_3_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_2_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_2_MONTH_IN_TIME").mean().collect()(0)(0).toString)
    val SCORE_1_MONTH_IN_TIME = toInt(configDf.groupBy("SCORE_1_MONTH_IN_TIME").mean().collect()(0)(0).toString)

    var score = spark.read.parquet("/user/MobiScore_Output/post_payment/score_model_dataframe.parquet")

    val second_Month = if(startMonth+ 1 <= 12) startMonth+1 else startMonth+1 -12;
    val third_Month = if(startMonth+ 2 <= 12) startMonth+2 else startMonth+2 -12;
    val fourth_Month = if(startMonth+ 3 <= 12) startMonth+3 else startMonth+3 -12;
    val fifth_Month = if(startMonth+ 4 <= 12) startMonth+4 else startMonth+4 -12;
    val sixth_Month = if(startMonth+ 5 <= 12) startMonth+5 else startMonth+5 -12;

    println(startMonth)
    println(second_Month)
    println(third_Month)
    println(fifth_Month)
    println(sixth_Month)

    score = score.withColumn("score_in_time", (score("sc_1_"+startMonth)+score("sc_1_"+(second_Month))+score("sc_1_"+(third_Month))+score("sc_1_"+(fourth_Month))+score("sc_1_"+(fifth_Month))+score("sc_1_"+(sixth_Month)))*SCORE_IN_TIME)
    score = score.withColumn("score_spare_payment", (score("sc_2_"+startMonth)+score("sc_2_"+(second_Month))+score("sc_2_"+(third_Month))+score("sc_2_"+(fourth_Month))+score("sc_2_"+(fifth_Month))+score("sc_2_"+(sixth_Month)))*SCORE_SPARE_PAYMENT)
    score = score.withColumn("score_avg_using", (score("sc_3_"+startMonth)+score("sc_3_"+(second_Month))+score("sc_3_"+(third_Month))+score("sc_3_"+(fourth_Month))+score("sc_3_"+(fifth_Month))+score("sc_3_"+(sixth_Month)))*SCORE_AVG_USING)
    score = score.withColumn("score_avg_paying", (score("sc_4_"+startMonth)+score("sc_4_"+(second_Month))+score("sc_4_"+(third_Month))+score("sc_4_"+(fourth_Month))+score("sc_4_"+(fifth_Month))+score("sc_4_"+(sixth_Month)))*SCORE_AVG_PAYING)
    score = score.withColumn("score_using_bank", (score("sc_5_"+startMonth)+score("sc_5_"+(second_Month))+score("sc_5_"+(third_Month))+score("sc_5_"+(fourth_Month))+score("sc_5_"+(fifth_Month))+score("sc_5_"+(sixth_Month)))*SCORE_USING_BANK)
    score = score.withColumn("score_not_using_packet", (score("sc_6_"+startMonth)+score("sc_6_"+(second_Month))+score("sc_6_"+(third_Month))+score("sc_6_"+(fourth_Month))+score("sc_6_"+(fifth_Month))+score("sc_6_"+(sixth_Month)))*SCORE_NOT_USING_PACKET)

    score = score.select("col0","score_in_time","score_spare_payment", "score_avg_using", "score_avg_paying", "score_using_bank", "score_not_using_packet")
    score = score.na.drop()

    // calculate 6 feature about paying in time
    score = score.withColumn("score_6_month_in_time", when(score("score_in_time") === 6*SCORE_IN_TIME, SCORE_6_MONTH_IN_TIME*6).otherwise(0))
    score = score.withColumn("score_5_month_in_time", when(score("score_in_time") === 5*SCORE_IN_TIME, SCORE_5_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_4_month_in_time", when(score("score_in_time") === 4*SCORE_IN_TIME, SCORE_4_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_3_month_in_time", when(score("score_in_time") === 3*SCORE_IN_TIME, SCORE_3_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_2_month_in_time", when(score("score_in_time") === 2*SCORE_IN_TIME, SCORE_2_MONTH_IN_TIME).otherwise(0))
    score = score.withColumn("score_1_month_in_time", when(score("score_in_time") === SCORE_IN_TIME, SCORE_1_MONTH_IN_TIME).otherwise(0))

    score = score.withColumn("score", lit(114)+score("score_in_time")+score("score_spare_payment")+score("score_avg_using")+score("score_avg_paying")+score("score_6_month_in_time")+score("score_5_month_in_time")+score("score_4_month_in_time")+score("score_3_month_in_time")+score("score_2_month_in_time")+score("score_1_month_in_time")+score("score_1_month_in_time") + score("score_using_bank")+ score("score_not_using_packet"))

    // select only score col
    score = score.select("col0", "score")
    score.write.mode("overwrite").parquet("/user/MobiScore_Output/post_payment/score_estimate.parquet")

    score.createOrReplaceTempView("score_predict")

//    val real_score = spark.read.parquet("/user/MobiScore_DataSource/M_SCORE/FileName=M_SCORE.txt")
//    real_score.createOrReplaceTempView("real_score")

//    val result = spark.sql("select count(*) from (select r._c0, _c12, s.score, (score/_c12*100) ratio from score_predict s, real_score r where s.col0 = r._c0 ) as tmp where tmp.ratio >= 25 and tmp.ratio <= 38")
//    result.show(false)
    println("Done")
  }
}
