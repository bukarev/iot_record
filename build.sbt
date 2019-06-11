name := "Consume IoT Azure by Spark"

version := "0.0.1"

scalaVersion := "2.11.0"

resolvers += "thirdparty-releases" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases/"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0.SP5"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0-RC1"
// https://mvnrepository.com/artifact/com.typesafe.play/play-ahc-ws-standalone
libraryDependencies += "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.0-M3"
// https://mvnrepository.com/artifact/com.typesafe.play/play-ws-standalone-json
libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-json" % "2.0.0-M3"
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-actor
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.15"
// https://mvnrepository.com/artifact/com.microsoft.azure.sdk.iot/iot-service-client
libraryDependencies += "com.microsoft.azure.sdk.iot" % "iot-service-client" % "1.14.1"
// https://mvnrepository.com/artifact/com.microsoft.azure.sdk.iot/iot-deps
libraryDependencies += "com.microsoft.azure.sdk.iot" % "iot-deps" % "0.7.0"
