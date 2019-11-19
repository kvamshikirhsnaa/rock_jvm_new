name := "rock_jvm"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "com.typesafe"%"config" % "1.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

// libraryDependencies += "com.crealytics" %% "spark-excel" % "0.9.8"

resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v0.26.0"

// https://mvnrepository.com/artifact/org.typelevel/frameless-core
libraryDependencies += "org.typelevel" %% "frameless-core" % "0.8.0"

libraryDependencies += "org.typelevel" %% "frameless-dataset" % "0.8.0"

libraryDependencies += "org.json" % "json" % "20190722"

// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "0.3.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.18"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"

// https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"




