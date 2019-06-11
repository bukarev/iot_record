import scala.util.matching.Regex

import org.apache.spark._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.sql.kafka010.KafkaSourceRDDPartition
import org.apache.spark.sql.types._

import org.apache.spark.sql._

import net.liftweb.json._
import net.liftweb.json.Serialization.write

import org.apache.spark.sql.functions.typedLit

object azureSensortag {

  def main(args: Array[String]) {

case class rawSignal(deviceId: String, offset: String, enqueuedTime: String, sequenceNumber: String, content: String)
case class iotSignal(deviceId: String, msgID: String, utctime: String, gyroX: String, gyroY: String, gyroZ: String, accX: String, accY: String, accZ: String)   

val Array(
              brokers,
              topics,
              groupId,
              offsetReset,
              batchInterval,
              pollTimeout,
              mode) = args

val topicsSet = topics.split(",").toSet

val spark = SparkSession
  .builder
  .appName("azureSensorTag")
  .getOrCreate()

import spark.implicits._

    val messages = spark.readStream
                   .format("kafka").
                   option("subscribe", topics).
                   option("kafka.bootstrap.servers", brokers).
                   option("group.id", groupId).
                   option("auto.offset.reset", offsetReset).
                   option("failOnDataLoss", "false").
                   load
   
    val messages1 = spark.readStream
                   .format("kafka").
                   option("subscribe", topics).
                   option("kafka.bootstrap.servers", brokers).
                   option("group.id", groupId).
                   option("auto.offset.reset", offsetReset).
                   option("failOnDataLoss", "false").
                   load               

    val analyseThis = spark.readStream
                   .format("kafka").
                  option("subscribe", topics).
                   option("kafka.bootstrap.servers", brokers).
                   option("group.id", groupId).
                   option("auto.offset.reset", offsetReset).
                   option("failOnDataLoss", "false").
                   load
                   
val jDeviceId = "deviceId=(.*?),".r
val jOffset = "offset=(.*?),".r
val jEnqueuedTime = "enqueuedTime=(.*?),".r
val jSequenceNumber = "sequenceNumber=(.*?),".r
val jContent = "content=(.*?)},".r

implicit val formats = DefaultFormats

val getDeviceId = udf((x: String) => (jDeviceId findFirstIn x).mkString.replace("deviceId=","").dropRight(1))
val getOffset = udf((x: String) => (jOffset findFirstIn x).mkString.replace("offset=","").dropRight(1))
val getEnqTime = udf((x: String) => (jEnqueuedTime findFirstIn x).mkString.replace("enqueuedTime=","").dropRight(1))
val getSeqNumber = udf((x: String) => (jSequenceNumber findFirstIn x).mkString.replace("sequenceNumber=","").dropRight(1).toInt)
val getContent = udf((x: String) => (jContent findFirstIn x).mkString.replace("content=","").dropRight(1))


val getGyroX = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroX.toDouble }) 
val getGyroY = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroY.toDouble })
val getGyroZ = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].gyroZ.toDouble })
val getAccX = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accX.toDouble }) 
val getAccY = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accY.toDouble })
val getAccZ = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].accZ.toDouble })
val getMsgID = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].msgID })
val getUTCTime = udf((x: String) => {implicit val formats = DefaultFormats; parse(x).extract[iotSignal].utctime.toString })

val outFile =  messages1
                       .filter($"topic".equalTo(typedLit("sensortag_raw")))
                       .select($"value" cast "string")
                        .withColumn("utcTime", getUTCTime(col("value")))
                        .withColumn("gyroX", getGyroX(col("value")))
                        .withColumn("gyroY", getGyroY(col("value")))
                        .withColumn("gyroZ", getGyroZ(col("value")))
                        .withColumn("accX", getAccX(col("value")))
                        .withColumn("accY", getAccY(col("value")))
                        .withColumn("accZ", getAccZ(col("value")))
                        .select("utcTime", "gyroX", "gyroY", "gyroZ", "accX", "accY", "accZ")
                        .writeStream
                        .outputMode("append")
                        .format("parquet")        // can be "orc", "json", "csv", etc.
                        .option("checkpointLocation", "/user/sensortag/checkpoint/")
                        .option("path", "/user/sensortag/in_raw")
                        .start()                                                                                      

val outConsole = messages
                       .select($"value" cast "string")
                        .writeStream
                        .outputMode("append")
                        .format("console")
                        .start()                        

    // Start the computation                        
    outConsole.awaitTermination()                    
    outFile.awaitTermination()

  }
}
