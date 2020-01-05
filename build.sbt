name := "spark-batching"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.0"
val kafkaVersion = "0.11.0.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion

libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion

libraryDependencies += "org.apache.commons" % "commons-exec" % "1.3"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.18"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion

libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"

libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.3"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.12.0" % Test

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v2.3.0_0.12.0" % "test"







