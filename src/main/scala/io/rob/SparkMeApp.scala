package io.rob;

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.util.{JSON, JSONParseException}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j._
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object DirectKafkaWordCount {
  val log = Logger.getRootLogger()


  def jsonStrToMongoObject(jsonStr: String): DBObject = {
    try {
      return JSON.parse(jsonStr).asInstanceOf[DBObject]
    } catch {
      case e: JSONParseException => {
        log.info("Invalid JSON" + e)
        log.warn("Invalid JSON returning null")
      }
        return null
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaToMongo")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print

    val lines = messages.map(_._2)
    lines.foreachRDD { (rdd: RDD[String], time: Time) =>
      val data = rdd.collect()
      val mongoClient = MongoClient("127.0.0.1", 27017)
      val db = mongoClient("test")

      if(data.length > 0) {
        for (json: String <- data) {
          log.info("message ====================================== \n" + json)
          val jsondb:DBObject =jsonStrToMongoObject(json);
          val metaString = jsondb.get("meta").asInstanceOf[DBObject].get("serverID").toString
          log.info("message ====================================== \n\n\n\n\n\n\n" + metaString + "\n\n\n   =============================   ")

          db.getCollection(metaString).insert(jsondb)

        }
      }
      mongoClient.close();
    }



    ssc.start()
    ssc.awaitTermination()
  }
}