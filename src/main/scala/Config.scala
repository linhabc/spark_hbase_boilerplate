import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Config {
  // hbase context config
  val QUORUM = "localhost"
  val PORT = 2181

  // file(s) path
  val FILE_PATH = "/home/linhnguyen/file.csv"

  // csv schema
  val CSV_SCHEMA= new StructType()
    .add("col0",StringType,true)
    .add("col2",StringType,true)
    .add("col3",StringType,true)
    .add("col4",StringType,true)
    .add("col5",StringType,true)
    .add("col6",StringType,true)
    .add("col7",StringType,true)
    .add("col8",StringType,true)

  // mapping dataframe to hbase table
  val TABLE_NAME = "phoneNum"
  val TABLE_SCHEMA = "col0 STRING :key ," +
                          "col2 STRING col1:col2, col3 STRING col1:col3, " +
                          "col4 STRING col1:col4, col5 STRING col1:col5," +
                          "col6 STRING col1:col6, col7 STRING col1:col7," +
                          "col8 STRING col1:col8"
}
