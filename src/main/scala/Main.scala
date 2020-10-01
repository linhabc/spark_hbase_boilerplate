import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.split

object Main{
  def main(args: Array [String]){

    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", Config.QUORUM)
    conf.setInt("hbase.zookeeper.property.clientPort", Config.PORT)
    conf.set("hbase.rootdir","/apps/hbase/data")
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf.set("hbase.cluster.distributed","true")

    new HBaseContext(spark.sparkContext, conf)

    var df = spark.read.parquet(Config.FILE_PATH)

    df.show(10, false)

    import spark.implicits._

    df = df.withColumn("tmp", split(df("_c0"), "\\|"))
    df = df.withColumn("id", df("tmp").getItem(0))
          .withColumn("col1", df("tmp").getItem(1))
          .withColumn("col2", df("tmp").getItem(2))
          .withColumn("col3", df("tmp").getItem(3))
          .withColumn("col4", df("tmp").getItem(4))
          .withColumn("col5", df("tmp").getItem(5))
          .withColumn("col6", df("tmp").getItem(6))
          .withColumn("col7", df("tmp").getItem(7))

    df = df.drop(df("_c0"))
    df = df.drop(df("tmp"))
    df = df.drop(df("FileName"))

    df.printSchema()

    df.show(30)

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table", Config.TABLE_NAME)
      .option("hbase.columns.mapping", Config.TABLE_SCHEMA)
      .save()

    println("Done")
  }
}

    // val df = spark.read.json(Config.FILE_PATH)

    // val sqlContext = spark.sqlContext
    // val df = sqlContext.read.format("com.databricks.spark.csv")
    //   .schema(Config.CSV_SCHEMA)
    //   .option("delimiter", "|")
    //   .load(Config.FILE_PATH)
