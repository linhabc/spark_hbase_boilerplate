import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Config {
  // hbase context config
  val QUORUM = "datamining-01,mbfscore-app01,mbfscore-app02"
  val PORT = 2181

  // file(s) path
  val FILE_PATH = "/user/MobiScore_DataSource/subscriber"

  // mapping dataframe to hbase table
  val TABLE_NAME = "hbase_db"
  val TABLE_SCHEMA = "id STRING :key ," +
                      "col1 DateTime mb_sc:ACTIVE_DATE, col2 DateTime mb_sc:EXPIRE_DATE, " +
                      "col3 STRING mb_sc:SUB_TYPE, col4 STRING mb_sc:SUB_PREFIX," +
                      "col5 INT mb_sc:AGE, col6 INT mb_sc:SEX," +
                      "col7 STRING mb_sc:SOURCE_SYS"
}

