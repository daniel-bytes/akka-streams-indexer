package com.example

import java.io.File

import akka.stream._
import akka.stream.scaladsl._
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import org.elasticsearch.client.RestClient
import org.apache.http.HttpHost
import org.squbs.pattern.stream.{PersistentBuffer, QueueSerializer}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration


// Running Elasticsearch:
// docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:6.4.2

case class Message(id: Int, value: Int) {
  override def toString: String = s"$id: $value"
}

object Message {
  val maxValue = 100000
  val random = scala.util.Random

  def apply(id: Int): Message = {
    Message(id, random.nextInt(maxValue).abs)
  }

  def generate(num: Int): Seq[Message] =
    for (i <- 1 to num) yield Message(i)
}

object MessageJsonProtocol extends DefaultJsonProtocol {
  implicit val format: JsonFormat[Message] = jsonFormat2(Message.apply)
}

object Main extends App {
  // Constants
  val numMessages = 1000
  val sourceBufferSize = Int.MaxValue
  val elasticsearchBufferSize = 5
  val elasticsearchMaxRetries = 5
  val elasticsearchRetryInterval: FiniteDuration = 1 second
  val elasticsearchIndexName = "test-1"
  val elasticsearchTypeName = "_doc"
  val elasticsearchHostname = "0.0.0.0"
  val elasticsearchPort = 9200
  val persistentBufferFilePath = "/tmp/akka-streams-indexer"
  val throttleElements = 10
  val throttleRate: FiniteDuration = 1 second

  // Implicits
  import MessageJsonProtocol._

  implicit val system = ActorSystem("Indexer")
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  implicit val client = RestClient.builder(new HttpHost(elasticsearchHostname, elasticsearchPort)).build
  implicit val serializer = QueueSerializer[Message]()

  // Stream setup
  val buffer = new PersistentBuffer[Message](new File(persistentBufferFilePath))

  val source = Source.actorRef[Message](sourceBufferSize, OverflowStrategy.fail)

  val sink = Flow[Message]
    .via(buffer.async)
    .map(msg => WriteMessage.createUpsertMessage(msg.id.toString, msg))
    .throttle(elements = throttleElements, per = throttleRate)
    .toMat(
      ElasticsearchSink.create[Message](
        indexName = elasticsearchIndexName,
        typeName = elasticsearchTypeName,
        settings = ElasticsearchWriteSettings()
          .withBufferSize(elasticsearchBufferSize)
          .withRetryLogic(RetryAtFixedRate(elasticsearchMaxRetries, elasticsearchRetryInterval))

      )
    )(Keep.right)
    .runWith(source)

  // Publish test data to Stream
  for (d <- Message.generate(num = numMessages)) {
    sink ! d
  }
}
