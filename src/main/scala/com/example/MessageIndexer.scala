package com.example

import java.io.File

import akka.stream._
import akka.stream.scaladsl._
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.elasticsearch.client.RestClient
import org.apache.http.HttpHost
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.squbs.pattern.stream.{PersistentBuffer, QueueSerializer}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

class MessageIndexer
(
  val sourceBufferSize: Int = Int.MaxValue,
  val elasticsearchBufferSize: Int = 5,
  val elasticsearchMaxRetries: Int = 5,
  val elasticsearchRetryInterval: FiniteDuration = 1 second,
  val elasticsearchIndexName: String = "test-write",
  val elasticsearchTypeName: String = "_doc",
  val elasticsearchHostname: String = "0.0.0.0",
  val elasticsearchPort: Int = 9200,
  val persistentBufferFilePath: String = "/tmp/akka-streams-indexer",
  val throttleElements: Int = 10,
  val throttleRate: FiniteDuration = 1 second
)(
  implicit val system: ActorSystem
) {
  import MessageJsonProtocol.format

  private implicit val materializer: Materializer = ActorMaterializer()
  private implicit val client = RestClient.builder(new HttpHost(elasticsearchHostname, elasticsearchPort)).build
  private implicit val serializer = QueueSerializer[Message]()
  private val mapper = new ObjectMapper()

  val actor = Flow[Message]
    // Write incoming messages to buffer
    .map(msg => { println(s"[$msg] - Buffering"); msg })
    .via(
      new PersistentBuffer[Message](new File(persistentBufferFilePath)).async
    )

    // Throttle incoming values (may be coming from buffer on a cold restart)
    .map(msg => { println(s"[$msg] - Throttling"); msg })
    .throttle(elements = throttleElements, per = throttleRate)

    // Index to Elasticsearch
    .map(msg => { println(s"[$msg] - Indexing"); msg })
    .map(msg => WriteMessage.createUpsertMessage(msg.id.toString, msg))

    .toMat(
      ElasticsearchSink.create[Message](
        indexName = elasticsearchIndexName,
        typeName = elasticsearchTypeName,
        settings = ElasticsearchWriteSettings()
          .withBufferSize(elasticsearchBufferSize)
          .withRetryLogic(RetryAtFixedRate(elasticsearchMaxRetries, elasticsearchRetryInterval))
      )
    )(Keep.left)
    .runWith(
      Source.actorRef[Message](sourceBufferSize, OverflowStrategy.fail)
    )

  def publish(msg: Message): Unit = {
    actor ! msg
  }

  def createIndex(): String = {
    val response =  client.performRequest("GET", "/_alias")
    val aliasResponse = mapper.readTree(response.getEntity.getContent).asInstanceOf[ObjectNode]
    println(aliasResponse.toString)

    val newIndex = getNextIndexName(aliasResponse)

    client.performRequest("PUT", newIndex)
    newIndex
  }

  def setAlias(index: String, forRead: Boolean = false, forWrite: Boolean = false): Unit = {
    if (!forRead && !forWrite) return

    val response =  client.performRequest("GET", "/_alias")
    val aliasResponse = mapper.readTree(response.getEntity.getContent).asInstanceOf[ObjectNode]
    println(aliasResponse.toString)

    def send(alias: String): Unit = {
      val currentIndex = getIndexWithAlias(aliasResponse, alias)
      val aliasBody = createAliasPayload(alias, currentIndex, index)

      client.performRequest(
        "POST",
        "_aliases",
        Map[String, String]().asJava,
        new StringEntity(aliasBody.toString),
        new BasicHeader("Content-Type", "application/json"))
    }

    if (forRead) {
      send("test-read")
    }

    if (forWrite) {
      send("test-write")
    }
  }

  private def getIndexWithAlias(aliasResponse: ObjectNode, alias: String): Option[String] = {
    aliasResponse.fields.asScala.find(
      _.getValue.asInstanceOf[ObjectNode].path("aliases").asInstanceOf[ObjectNode].fieldNames.asScala.contains(alias)
    ).map(_.getKey)
  }

  private def getNextIndexName(aliasResponse: ObjectNode): String = {
    val maxVal = aliasResponse.fieldNames.asScala.toList
      .filter(_.startsWith("test-"))
      .map(_.split('-').reverse.head.toInt)
      .sortBy(x => x)
      .reverse
      .headOption
      .getOrElse(0)

    s"test-${maxVal + 1}"
  }

  private def createAliasPayload(
    alias: String,
    maybeCurrentIndex: Option[String],
    newIndex: String
  ): ObjectNode = {
    val aliasBody = mapper.createObjectNode()
    val actions = aliasBody.putArray("actions")

    val addAction = mapper.createObjectNode()
    addAction
      .putObject("add")
      .put("index", newIndex)
      .put("alias", alias)
    actions.add(addAction)

    maybeCurrentIndex.map(currentIndex => {
      if (currentIndex != newIndex) {
        val removeAction = mapper.createObjectNode()
        removeAction
          .putObject("remove")
          .put("index", currentIndex)
          .put("alias", alias)
        actions.add(removeAction)
      }
    })

    println(aliasBody.toString)
    aliasBody
  }
}

