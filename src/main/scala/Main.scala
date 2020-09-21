import org.apache.spark.sql.SparkSession

// TODO: replace
case class Person(name: String,
                  email: String,
                  age: String,
                  height: Double,
                  university: String)

object Main{
  def main(args: Array [String]){
    val spark = SparkSession
      .builder()
      .appName("Spark with hbase")
      .getOrCreate()

    // TODO: replace
    val filepath = "file.json"
    val tableName = "person"
    val hbaseTableMapping = "name STRING :key, email STRING c:email, " +
                            "age DATE p:age, height DOUBLE p:height, " +
                            "university String school:university"

    import spark.implicits._
    val df = spark.read.json(filepath).as[Person] // may use dataframe

    df.printSchema()

    df.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.table", tableName)
      .option("hbase.columns.mapping", hbaseTableMapping)
      .option("hbase.spark.use.hbasecontext", false)
      .save()

    println("done")
  }
}

