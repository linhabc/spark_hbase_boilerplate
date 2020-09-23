object Config {
  // hbase context config
  val QUORUM = "localhost"
  val PORT = 2181

  // file path
  val FILE_PATH = "file.json"

  // table characteristics
  val TABLE_NAME = "person"
  val TABLE_SCHEMA = "name STRING :key, email STRING c:email, " +
                          "age DATE p:age, height DOUBLE p:height, " +
                          "university String school:university"
}
