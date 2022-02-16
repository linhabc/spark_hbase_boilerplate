import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, count, sum, when}

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
   val MONTH = (configDf.groupBy("MONTH").mean().collect()(0)(0).toString)
   val YEAR = (configDf.groupBy("YEAR").mean().collect()(0)(0).toString)

   val conf = HBaseConfiguration.create()
   conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
   conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
   conf.set("hbase.rootdir","/apps/hbase/data")
   conf.set("zookeeper.znode.parent","/hbase-unsecure")
   conf.set("hbase.cluster.distributed","true")

   new HBaseContext(spark.sparkContext, conf)

    var total_user = spark.read.parquet("/user/MobiScore_Output/subscriber.parquet")
    total_user.createOrReplaceTempView("total_user")
    total_user.count
    total_user.show(false)

    total_user = total_user.dropDuplicates("col0")

    var length_credit = spark.read.parquet("/user/MobiScore_Output/length_credit_his_2")
    length_credit.show(false)
    // length_credit = length_credit.withColumn("LENGTH_CREDIT_HIS_SCORE", when(col("LENGTH_CREDIT_HIS_SCORE") < 45, 45).otherwise(col("LENGTH_CREDIT_HIS_SCORE")))
    length_credit.count
    length_credit.createOrReplaceTempView("length_credit")


    var credit_mix = spark.read.parquet("/user/MobiScore_Output/credit_mix_2")
    credit_mix.show(false)
    // credit_mix = credit_mix.withColumn("SCORE_CREDIT_MIX", when(col("SCORE_CREDIT_MIX") < 45, 45).otherwise(col("SCORE_CREDIT_MIX")))
    credit_mix.count()
    credit_mix.createOrReplaceTempView("credit_mix")

    var pre_payment = spark.read.parquet("/user/MobiScore_Output/pre_payment/score_"+YEAR+MONTH+".parquet")
    pre_payment = pre_payment.select("isdn", "score")
    pre_payment = pre_payment.withColumnRenamed("isdn", "col0")

    var pre_payment_rev = spark.read.parquet("/user/MobiScore_Output/pre_payment/pre_payment_revenue_"+YEAR+MONTH+".parquet")
    pre_payment_rev = pre_payment_rev.select("_c0", "score")
    pre_payment_rev = pre_payment_rev.withColumnRenamed("isdn", "col0")
    pre_payment_rev.show(false)
    pre_payment_rev.count()

    pre_payment = pre_payment.union(pre_payment_rev)

    pre_payment.count()
    pre_payment.show(false)
    pre_payment.createOrReplaceTempView("pre_payment")

    var post_payment = spark.read.parquet("/user/MobiScore_Output/post_payment/score_estimate.parquet")
    post_payment = post_payment.filter(post_payment("score") > 114)

    total_user = total_user.withColumnRenamed("col0", "_c0")

    post_payment = post_payment.join(total_user, post_payment("col0") === total_user("_c0"))

    post_payment = post_payment.drop("_c0")

    total_user = total_user.withColumnRenamed("_c0", "col0")

    post_payment.count()
    post_payment.show(false)
    post_payment.createOrReplaceTempView("post_payment")

    var payment_history = pre_payment.union(post_payment)
    payment_history = payment_history.groupBy("col0").agg(sum("score").as("score"), count("*").as("count"))
    payment_history = payment_history.filter(payment_history("count") === 1)
    payment_history = payment_history.drop("count")
    // payment_history.show(false)
    // payment_history.count
    payment_history.createOrReplaceTempView("payment_history")

    payment_history = spark.sql("select ph.col0, score from payment_history ph, total_user tu where tu.col0 = ph.col0")
    payment_history = payment_history.dropDuplicates("col0")
    payment_history.show(false)
    payment_history.count
    payment_history.createOrReplaceTempView("payment_history")

    payment_history.write.mode("overwrite").parquet("/user/MobiScore_Output/payment_history.parquet")

    var amount_owned_pos = spark.read.parquet("/user/MobiScore_Output/amount_owed/amount_owed-scoring-pos-"+YEAR+MONTH+".parquet")
    amount_owned_pos = amount_owned_pos.select("ISDN", "AO_SCORE")
   amount_owned_pos = amount_owned_pos.withColumnRenamed("AO_SCORE", "SCORE")

   amount_owned_pos.count()
    var amount_owned_pre = spark.read.parquet("/user/MobiScore_Output/amount_owed/amount_owed-scoring-pre-"+YEAR+MONTH+".parquet")
    amount_owned_pre.count()
    amount_owned_pre = amount_owned_pre.select("ISDN", "AO_SCORE")
   amount_owned_pre = amount_owned_pre.withColumnRenamed("AO_SCORE", "SCORE")
    var amount_owned = amount_owned_pre.union(amount_owned_pos)
    // amount_owned = amount_owned.withColumn("ao_score", when(col("ao_score") < 99, 99).otherwise(col("ao_score")))
    amount_owned.show(false)
    amount_owned.count()
    amount_owned.createOrReplaceTempView("amount_owned")

    var total_score = spark.sql("select ph.col0, LENGTH_CREDIT_HIS_SCORE,  ph.score as PAYMENT_HISTORY_SCORE, ao.SCORE as AMOUNT_OWED_SCORE, cm.SCORE_CREDIT_MIX as CREDIT_MIX_SCORE from payment_history ph left join length_credit lc on ph.col0 = lc._c0 left join amount_owned ao on ph.col0 = ao.ISDN left join credit_mix cm on ph.col0 = cm.new_id")
    total_score = total_score.na.fill(0)
    total_score = total_score.withColumn("PAYMENT_HISTORY_SCORE", when(col("PAYMENT_HISTORY_SCORE") < 114, 114).otherwise(col("PAYMENT_HISTORY_SCORE")))
    total_score = total_score.withColumn("AMOUNT_OWED_SCORE", when(col("AMOUNT_OWED_SCORE") < 99, 99).otherwise(col("AMOUNT_OWED_SCORE")))
    total_score = total_score.withColumn("LENGTH_CREDIT_HIS_SCORE", when(col("LENGTH_CREDIT_HIS_SCORE") < 45, 45).otherwise(col("LENGTH_CREDIT_HIS_SCORE")))
    total_score = total_score.withColumn("CREDIT_MIX_SCORE", when(col("CREDIT_MIX_SCORE") < 45, 45).otherwise(col("CREDIT_MIX_SCORE")))

    total_score.count()
    total_score.createOrReplaceTempView("total_score")
    total_score = spark.sql("select col0, LENGTH_CREDIT_HIS_SCORE, PAYMENT_HISTORY_SCORE, AMOUNT_OWED_SCORE, CREDIT_MIX_SCORE, (LENGTH_CREDIT_HIS_SCORE + PAYMENT_HISTORY_SCORE + AMOUNT_OWED_SCORE + CREDIT_MIX_SCORE) as TOTAL_SCORE  from total_score")
    total_score.createOrReplaceTempView("total_score")
    total_score.show(false)


    total_score.write.mode("overwrite").parquet("/user/MobiScore_Output/"+MONTH+YEAR+"_score_output.parquet")

   println("Done")
  }
}