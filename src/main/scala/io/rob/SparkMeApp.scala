package io.rob;

import org.mongodb.scala.{Completed, MongoClient, MongoCollection, MongoDatabase, Observer}
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonNumber, BsonObjectId}
import java.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j._

import scala.collection.mutable
import scala.concurrent.Await
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

  def jsonStrToMongoObject(jsonStr: String): BsonDocument = {
    val doc = BsonDocument.apply(jsonStr)
    return  doc
  }
  def toArray(docuemt:AnyRef): Array[BsonDocument] ={
    return docuemt.asInstanceOf[BsonArray].asArray().toArray().map(_.asInstanceOf[BsonDocument])
  }

  def sendToMongo(col_name:String, doc:BsonDocument): Unit ={
    val mongoClient = MongoClient("mongodb://192.168.1.173:27017")
    val database: MongoDatabase = mongoClient.getDatabase("beta")
    val collection: MongoCollection[BsonDocument] = database.getCollection(col_name);
    val req = collection.insertOne(doc)

    req.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = { println("Inserted"); }
      override def onError(e: Throwable): Unit = { println(" \n\nFailed " + e + "\n\n"); }
      override def onComplete(): Unit = { println("Completed"); }
    })
    try {
      Await.result(req.toFuture, scala.concurrent.duration.Duration("5 second"))
    }catch{
      case e: util.concurrent.TimeoutException =>{println("Error: "+e)}
      case e: Exception =>{}
    }

    mongoClient.close();
  }
  def processMessage(json:String): Unit ={

    val jsondb: BsonDocument = jsonStrToMongoObject(json);
    val timestamp: Int = (System.currentTimeMillis / 1000).toInt
    jsondb.get("meta").asDocument().append("time", BsonNumber(timestamp))
    val metaString = jsondb.get("meta").asInstanceOf[BsonDocument].get("serverID").asString().getValue

    if (jsondb.get("ServerMeta") != null) {
      val Worlds: Array[BsonDocument] = jsondb.get("Worlds").asArray().toArray().map(_.asInstanceOf[BsonDocument])
      Worlds.foreach(w => {
        val chunks = toArray(w.get("loadedChunks"))
        val worldname: String = w.get("worldName").asString().getValue
        val chunkList = mutable.MutableList[BsonObjectId]()
        val clist = BsonArray()
        val worldDoc = w;
        worldDoc.remove("loadedChunks");
        println("WORLD:" + worldDoc.toJson())
        chunks.foreach(c => {
          println(c.toJson)
          val col_name = metaString + "_chunks"
          val id = BsonObjectId.apply()
          c.append("_id", id)
          println(id)
          val doc = new BsonDocument;
          doc.append("id", id)
          doc.append("location", c.get("location").asInstanceOf[BsonDocument])
          c.append("time", BsonNumber(timestamp))
          clist.add(doc)
          sendToMongo(col_name, c)
        })
        val list: Seq[BsonObjectId] = chunkList.toList
        worldDoc.append("chunks", clist)
        var col_name = metaString + "_worlds";
        sendToMongo(col_name, worldDoc)

        val doc = jsondb.get("ServerMeta").asDocument();
        doc.append("time", BsonNumber(timestamp))

        col_name = metaString + "_events";
        sendToMongo(col_name, doc)
      })
    }else{
      val col_name = metaString + "_events";
      sendToMongo(col_name, jsondb);
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
      if(data.length > 0) {
        for (json: String <- data) {
          processMessage(json);
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
