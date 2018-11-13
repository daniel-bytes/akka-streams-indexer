package com.example

import akka.stream._
import akka.stream.scaladsl._
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import org.elasticsearch.client.RestClient
import org.apache.http.HttpHost
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

  def generate(num: Int): Map[Int, Message] = {
    for (i <- 1 to num) yield {
      i -> Message(i)
    }
  }.toMap
}

object MessageJsonProtocol extends DefaultJsonProtocol {
  implicit val format: JsonFormat[Message] = jsonFormat2(Message.apply)
}

class IndexingActor(
  queueBufferSize: Int = 5,
  elasticsearchBufferSize: Int = 5,
  elasticsearchMaxRetries: Int = 5,
  elasticsearchRetryInterval: FiniteDuration = 1 second
)
(
  implicit materializer: Materializer,
  ec: ExecutionContext,
  client: RestClient,
  format: JsonFormat[Message]
) extends Actor {

  val stream = Source
    .queue[Message](bufferSize = queueBufferSize, OverflowStrategy.dropBuffer)
    .throttle(elements = queueBufferSize * 10, per = 1.seconds)
    .map(msg => WriteMessage.createUpsertMessage(msg.id.toString, msg))
    .toMat(
      ElasticsearchSink.create[Message](
        indexName = "test-1",
        typeName = "_doc",
        settings = ElasticsearchWriteSettings()
            .withBufferSize(elasticsearchBufferSize)
            .withRetryLogic(RetryAtFixedRate(elasticsearchMaxRetries, elasticsearchRetryInterval))

      )
    )(Keep.left)
    .run()

  override def receive: Receive = {
    case msg: Message => {
      println(s"Actor received message $msg")
      stream.offer(msg).map {
        case QueueOfferResult.Enqueued    ⇒ println(s"- enqueued $msg")
        case QueueOfferResult.Dropped     ⇒ println(s"- dropped $msg")
        case QueueOfferResult.Failure(ex) ⇒ println(s"- offer failed ${ex.getMessage}")
        case QueueOfferResult.QueueClosed ⇒ println("- source Queue closed")
      }
    }
  }
}

object Main extends App {
  import MessageJsonProtocol._

  val numMessages = 1000

  implicit val system = ActorSystem("Indexer")
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec = system.dispatcher
  implicit val client = RestClient.builder(new HttpHost("0.0.0.0", 9200)).build

  val data = Message.generate(num = numMessages)
  val actor = system.actorOf(Props[IndexingActor](new IndexingActor()), name = "indexer")

  for (d <- data.values.toList.sortBy(x => x.id)) {
    actor ! d
  }
}
