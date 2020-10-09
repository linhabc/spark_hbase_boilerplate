import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions._
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

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", configDf.groupBy("QUORUM").mean().collect()(0)(0).toString)
    conf.set("hbase.zookeeper.property.clientPort", configDf.groupBy("PORT").mean().collect()(0)(0).toString)
    conf.set("hbase.rootdir","/apps/hbase/data")
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf.set("hbase.cluster.distributed","true")

    new HBaseContext(spark.sparkContext, conf)

    var df = spark.read.parquet(configDf.groupBy("FILE_PATH").mean().collect()(0)(0).toString)
//    `ISDN` string,`MONTH` string,`REVENUE` string,`msc` string,`SMS` string,`VAS` string,`DATA` string

    df = df.select("_c0","_c1", "_c2")
    df = df.withColumnRenamed("_c0", "ISDN")
           .withColumnRenamed("_c1", "MONTH")
           .withColumnRenamed("_c2", "REVENUE")

    df = df.groupBy("ISDN").agg(expr("max(MONTH)").as("MAX_DATE"), expr("min(MONTH)").as("MIN_DATE"),
                                      expr("max(REVENUE)").as("MAX_REVENUE"), expr("min(REVENUE)").as("MIN_REVENUE"),
                                      expr("avg(REVENUE)").as("AVG_REVENUE"))

    df = df.withColumn("AVG_REVENUE", col("AVG_REVENUE").cast(IntegerType))
    df = df.withColumn("AVG_REVENUE", col("AVG_REVENUE").cast(StringType))

    df = df.drop("MONTH").drop("REVENUE")

    df.printSchema()

    df.show(10)

    df.write.mode("overwrite").parquet("/user/MobiScore_Output/revenue.parquet")

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table", configDf.groupBy("TABLE_NAME").mean().collect()(0)(0).toString)
      .option("hbase.columns.mapping", configDf.groupBy("TABLE_SCHEMA").mean().collect()(0)(0).toString)
      .save()

    println("Done")
  }
}