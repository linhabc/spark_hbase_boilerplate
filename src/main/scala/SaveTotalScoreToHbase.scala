import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SaveTotalScoreToHbase{
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

    var df = spark.read.parquet("/user/MobiScore_Output/score.parquet")

    df = df.withColumn("LENGTH_CREDIT_HIS_SCORE", col("LENGTH_CREDIT_HIS_SCORE").cast(IntegerType))
    df = df.withColumn("LENGTH_CREDIT_HIS_SCORE", col("LENGTH_CREDIT_HIS_SCORE").cast(StringType))

    df = df.withColumn("PAYMENT_HISTORY_SCORE", col("PAYMENT_HISTORY_SCORE").cast(IntegerType))
    df = df.withColumn("PAYMENT_HISTORY_SCORE", col("PAYMENT_HISTORY_SCORE").cast(StringType))

    df = df.withColumn("AMOUNT_OWED_SCORE", col("AMOUNT_OWED_SCORE").cast(IntegerType))
    df = df.withColumn("AMOUNT_OWED_SCORE", col("AMOUNT_OWED_SCORE").cast(StringType))

    df = df.withColumn("CREDIT_MIX_SCORE", col("CREDIT_MIX_SCORE").cast(IntegerType))
    df = df.withColumn("CREDIT_MIX_SCORE", col("CREDIT_MIX_SCORE").cast(StringType))

    df = df.withColumn("TOTAL_SCORE", col("TOTAL_SCORE").cast(IntegerType))
    df = df.withColumn("TOTAL_SCORE", col("TOTAL_SCORE").cast(StringType))

    df = df.withColumnRenamed("_c0", "ISDN")

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table", configDf.groupBy("TABLE_NAME").mean().collect()(0)(0).toString)
      .option("hbase.columns.mapping", configDf.groupBy("TABLE_SCHEMA").mean().collect()(0)(0).toString)
      .save()
    println("Done")

  }
}
