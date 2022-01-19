package kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/*
object SimpleKafka extends Serializable {
  def main(args: Array[String]): Unit = {
      println("SimpleKafka is up")
  }
}
*/

object SimpleKafka extends Serializable {
  def main(args: Array[String]): Unit = {

    val _kafkaTopic = "DebugData"
    val _kafkaUrl = "localhost:29092"

    val spark = SparkSession.builder()
      .appName("SimpleKafka")
      .master("local[3]")
      //      .master("spark://127.0.0.1:7077")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val schemaPhoto = StructType(List(
      StructField("albumId", LongType),
      StructField("id", LongType),
      StructField("title", StringType),
      StructField("url", StringType),
      StructField("thumbnailUrl", StringType),

    ))

    val schemaUser = StructType(List(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("username", StringType),
      StructField("email", StringType),
      StructField("address", StructType(List(
        StructField("street", StringType),
        StructField("suite", StringType),
        StructField("city", StringType),
        StructField("zipcode", StringType),
        StructField("geo", StructType(List(
          StructField("lat", StringType),
          StructField("lng", StringType)
        )))
      ))),
      StructField("phone", StringType),
      StructField("website", StringType),
      StructField("company", ArrayType(StructType(List(
        StructField("name", StringType),
        StructField("catchPhrase", StringType),
        StructField("bs", DoubleType)
      )))),
    ))

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", _kafkaUrl)
      .option("subscribe", _kafkaTopic)
      .option("startingOffsets", "latest")
      //      .option("startingOffsets", "earliest")
      .load()

    val schema = schemaPhoto

    val valueDF = kafkaDF.select(from_json(col("value").cast("string"), schema).alias("value"))

    //    val processedDf = valueDF.selectExpr("value.id", "value.name", "value.username", "value.email"
    //      , "value.address.street as street", "value.address.suite as suite", "value.address.suite as city"
    //      , "value.address.zipcode as zipcode")

    val processedDf = valueDF.selectExpr("value.id", "value.title", "value.thumbnailUrl")

    //    valueDF.printSchema()
    //    explodeDF.printSchema()

    val streamWriterQuery =
    //      explodeDF.writeStream
      processedDf.writeStream
        .queryName("Flattened Data Writer")
        .outputMode("append")
        .format("console")
        //      .format("json")
        .option("path", "output")
        .option("checkpointLocation", "chk-point-dir")
        //      .trigger(Trigger.ProcessingTime("1 minute"))
        .trigger(Trigger.ProcessingTime(5000))
        .start()

    //    logger.info("Listening to Kafka")
    streamWriterQuery.awaitTermination()
  }

}
