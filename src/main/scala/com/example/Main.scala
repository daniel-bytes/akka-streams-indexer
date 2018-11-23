package com.example

import akka.actor.ActorSystem

import scala.concurrent.Future
import scala.util.Random
import java.util.logging.{Level, LogManager}

// Running Elasticsearch:
// docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:6.4.2

object Main extends App {
  setLogging

  implicit val system = ActorSystem("Indexer")
  implicit val ec = system.dispatcher

  val numMessages = 20
  var publishing = true
  val messages = (for (i <- 1 to numMessages) yield Message(i, Haikunator.haiku)).toArray
  val indexer = new MessageIndexer()

  val idx = indexer.createIndex()
  println(s"**** Created initial index $idx")

  indexer.setAlias(idx, forRead = true, forWrite = true)
  println(s"**** Set read and write alias for $idx")

  println(s"**** Indexing data to $idx")
  val publisher = publish

  Thread.sleep(3000)

  println(s"**** Suspending writes")
  indexer.suspend()

  println(s"**** Reindexing")

  val newIdx = indexer.reindex(idx)
  println(s"**** Created new index $newIdx")

  Thread.sleep(2000)

  indexer.setAlias(newIdx, forWrite = true)
  println(s"**** Set write alias for $newIdx")

  println(s"**** Resuming writes")
  indexer.resume()

  Thread.sleep(2000)

  indexer.setAlias(newIdx, forRead = true, forWrite = true)
  println(s"**** Set read and write alias for $newIdx")

  Thread.sleep(2000)

  println(s"**** Done publishing")
  publishing = false
  system.terminate()
  System.exit(0)


  /**
    * Helper Functions
    */

  def publish = Future {
    val random = new Random()

    while(publishing) {
      val idx = Math.abs(random.nextInt()) % numMessages
      val sleep = (random.nextDouble() * 1000.0).toLong
      val doModify = Math.abs( random.nextInt() % 100 ) >= 95

      if (doModify) {
        val msg = messages(idx)
        val version = msg.version + 1
        val name = s"${msg.name}-$version"
        messages(idx) = msg.copy(name = name, version = version)
      }

      indexer.publish(messages(idx))

      Thread.sleep(sleep)
    }
  }

  def setLogging = {
    val rootLogger = LogManager.getLogManager.getLogger("")
    rootLogger.setLevel(Level.SEVERE)
    for (h <- rootLogger.getHandlers) {
      h.setLevel(Level.SEVERE)
    }
  }
}