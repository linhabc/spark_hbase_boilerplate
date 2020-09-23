import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration

object Main{
  def main(args: Array [String]){

    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", Config.QUORUM)
    conf.setInt("hbase.zookeeper.property.clientPort", Config.PORT)
    new HBaseContext(spark.sparkContext, conf)

    val df = spark.read.json(Config.FILE_PATH)

    df.printSchema()

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table", Config.TABLE_NAME)
      .option("hbase.columns.mapping", Config.TABLE_SCHEMA)
      .save()

    println("Done")
  }
}

