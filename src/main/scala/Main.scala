import org.apache.spark.sql.SparkSession



object Main{
  case class Person(name: String,
                    email: String,
                    age: String,
                    height: Double)

  def main(args: Array [String]){
    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()


    val filepath = "file.json" //replace

    import spark.implicits._
    val df = spark.read.json(filepath).as[Person]

    df.printSchema()

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping",
        "name STRING :key, email STRING c:email, " +
          "age DATE p:age, height FLOAT p:height")
      .option("hbase.table", "person")
      .option("hbase.spark.use.hbasecontext", false)
      .save()

    println("done")
  }
}

