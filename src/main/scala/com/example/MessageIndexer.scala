package com.example

import java.io.File

import akka.stream._
import akka.stream.scaladsl._
import akka.actor.{ActorSystem, PoisonPill}
import akka.stream.scaladsl.Source
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.elasticsearch.client.RestClient
import org.apache.http.HttpHost
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
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
  private val toggle = new ToggleState(initialState = true)

  /**
    * Configure our Akka Stream (see "Akka Stream Flow" in readme.md)
    */
  // 1. Receive messages from an Actor Source
  val source = Source.actorRef[Message](sourceBufferSize, OverflowStrategy.fail)

  val actor = Flow[Message]
    // 2. Write incoming messages to [[PersistentBuffer]]
    .map(msg => { println(s"[$msg] - Buffering"); msg })
    .via(
      new PersistentBuffer[Message](new File(persistentBufferFilePath)).async
    )
    // 3. Conditionally toggle back pressure
    .via(
      new Toggle[Message]().withAttributes(ToggleAttributes.toggleState(toggle))
    )

    // 4. Throttle incoming values (may be coming from buffer on a cold restart)
    .map(msg => { println(s"[$msg] - Throttling"); msg })
    .throttle(elements = throttleElements, per = throttleRate)

    // 5. Transform Message to an Elasticsearch [[WriteMessage]]
    .map(msg => { println(s"[$msg] - Indexing"); msg })
    .map(msg => WriteMessage.createIndexMessage(msg.id.toString, msg).withVersion(msg.version))

    // 6. Batch index data to Elasticsearch
    .toMat(
      ElasticsearchSink.create[Message](
        indexName = elasticsearchIndexName,
        typeName = elasticsearchTypeName,
        settings = ElasticsearchWriteSettings()
          .withBufferSize(elasticsearchBufferSize)
          .withRetryLogic(RetryAtFixedRate(elasticsearchMaxRetries, elasticsearchRetryInterval))
          .withVersionType("external")
      )
    )(Keep.left)
    .runWith(
      Source.actorRef[Message](sourceBufferSize, OverflowStrategy.fail)
    )

  /**
    * Suspends indexing of messages into Elasticsearch.
    * All incoming data will be buffered on disk in [[persistentBufferFilePath]]
    */
  def suspend(): Unit = {
    toggle.off()
  }

  /**
    * Resumes indexing of messages into Elasticsearch.
    * Any buffered messages will be streamed into Elasticsearch as well as new incoming messages.
    */
  def resume(): Unit = {
    toggle.on()
  }

  /**
    * Publishes a message into the Stream.
    */
  def publish(msg: Message): Unit = {
    actor ! msg
  }

  /**
    * Creates a new Elasticsearch index
    * @return The new Elasticsearch index name
    */
  def createIndex(): String = {
    val response =  client.performRequest("GET", "/_alias")
    val aliasResponse = mapper.readTree(response.getEntity.getContent).asInstanceOf[ObjectNode]

    val newIndex = getNextIndexName(aliasResponse)
    val body = mapper.createObjectNode()

    body
      .putObject("settings")
      .putObject("index")
      .put("number_of_shards", 1)
      .put("number_of_replicas", 1)

    val createIndexResponse = client.performRequest(
      "PUT",
      newIndex,
      Map[String, String]().asJava,
      new StringEntity(body.toString),
      new BasicHeader("Content-Type", "application/json"))

    println(s"create response: ${createIndexResponse.getStatusLine.getStatusCode} - ${EntityUtils.toString(createIndexResponse.getEntity)}")

    newIndex
  }

  /**
    * Reindexes an Elasticsearch index
    * @param source The source index
    * @param dest The destination index
    * @return The destination index name
    */
  def reindex(source: String, dest: String): String = {
    val body = createReindexPayload(source, dest)

    val reindexResponse = client.performRequest(
      "POST",
      "_reindex",
      Map[String, String]().asJava,
      new StringEntity(body.toString),
      new BasicHeader("Content-Type", "application/json"))

    println(s"reindex response: ${reindexResponse.getStatusLine.getStatusCode} - ${EntityUtils.toString(reindexResponse.getEntity)}")

    dest
  }

  /**
    * Sets read and/or write aliases for an index.
    * Previous alias assingments will be removed.
    * @param index The index to alias
    * @param forRead True if we want to set the read alias for this index
    * @param forWrite True if we want to set the write alias for this index
    */
  def setAlias(index: String, forRead: Boolean = false, forWrite: Boolean = false): Unit = {
    if (!forRead && !forWrite) return

    val response =  client.performRequest("GET", "/_alias")
    val aliasResponse = mapper.readTree(response.getEntity.getContent).asInstanceOf[ObjectNode]

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

  /**
    * Terminates the source Actor by sending a [[PoisonPill]]
    */
  def terminate(): Unit = {
    actor ! PoisonPill
  }

  /**
    * Returns the index associated with an alias, or [[None]] if not mapped
    * @param aliasResponse The Elasticsearch `GET /_alias` JSON response
    * @param alias The alias name to search
    * @return The index name mapped to the alias, or [[None]]
    */
  private def getIndexWithAlias(aliasResponse: ObjectNode, alias: String): Option[String] = {
    aliasResponse.fields.asScala.find(
      _.getValue.asInstanceOf[ObjectNode].path("aliases").asInstanceOf[ObjectNode].fieldNames.asScala.contains(alias)
    ).map(_.getKey)
  }

  /**
    * Returns the next index name to use
    * @param aliasResponse The Elasticsearch `GET /_alias` JSON response
    * @return The index name
    */
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

  /**
    * Creates a JSON payload to send to `POST /_aliases`
    * @param alias The alias name
    * @param maybeCurrentIndex The current index name mapped to the alias, or [[None]]
    * @param newIndex The new index name to map to the alias
    * @return The JSON payload
    */
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

  /**
    * Creates a JSON payload to send to `POST /_reindex`
    * @param source The source index
    * @param dest The destination index
    * @return The JSON payload
    */
  private def createReindexPayload(
    source: String,
    dest: String
  ): ObjectNode = {
    val body = mapper.createObjectNode()

    body
      .putObject("source")
      .put("index", source)

    body
      .putObject("dest")
      .put("index", dest)

    body
  }
}

