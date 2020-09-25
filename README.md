# spark_hbase_boilerplate
Install and run hbase in docker
-----
- docker pull harisekhon/hbase
- docker run -d -it --name=hbase -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16030:16030 -p 16201:16201 -p 16301:16301 -v ${PWD}/data:/hbase-data harisekhon/hbase
- copy host address in etc\hosts from running docker container to outside hosts file

Run spark application
-----
- sbt assembly
- ${spark-install-lib}/spark/bin/spark-submit --class "Main" --master local[4] target/scala-2.11/spark_hbase.jar

Link
-----
- http://localhost:16010/master-status for the Master Server

