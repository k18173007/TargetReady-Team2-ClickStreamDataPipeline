name := "TargetReady-Team2-ClickStreamDatatPipeline"

version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.17"

val sparkVersion: String = "3.4.1"
val sparkSqlConnectorVersion: String = "1.2.0"
val mysqlJavaConnectorVersion: String = "8.0.33"

libraryDependencies ++= Seq(

  //  Spark core libraries
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "com.typesafe" % "config" % "1.4.1",

  //  Streaming libraries
  "org.apache.kafka" % "kafka-clients" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  //  MySql Connector libraries
  "com.microsoft.azure" %% "spark-mssql-connector" % sparkSqlConnectorVersion,
  "mysql" % "mysql-connector-java" % mysqlJavaConnectorVersion,

  //  logging library
  "org.slf4j" % "slf4j-api" % "2.0.5",

  //  Testing library
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,

  //  Config props reader library
  "com.typesafe" % "config" % "1.4.2",

  //  PostgreSQL Connector libraries
  "org.postgresql" % "postgresql" % "42.3.1"
)


