package com.example

import akka.actor.ActorSystem

import scala.concurrent.Future
import scala.util.Random
import java.util.logging.{Level, LogManager}

import scala.concurrent.duration._

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

  pause()

  val newIdx = indexer.createIndex()
  println(s"**** Created new index $idx")

  println(s"**** Suspending writes")
  indexer.suspend()

  println(s"**** Reindexing")
  indexer.reindex(idx, newIdx)

  pause()

  indexer.setAlias(newIdx, forWrite = true)
  println(s"**** Set write alias for $newIdx")

  println(s"**** Resuming writes")
  indexer.resume()

  pause()

  indexer.setAlias(newIdx, forRead = true, forWrite = true)
  println(s"**** Set read and write alias for $newIdx")

  pause()

  println(s"**** Done publishing")
  publishing = false

  pause(1 second)

  println(s"**** Ending application")
  system.terminate()

  sys.exit(0)


  /**
    * Helper Functions
    */
  def publish = Future {
    val random = new Random()

    while(publishing) {
      val idx = Math.abs(random.nextInt()) % numMessages
      val sleep = (random.nextDouble() * 1000.0).toLong

      messages(idx) = messages(idx).copy(version = messages(idx).version + 1)
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

  def pause(duration: FiniteDuration = 5 seconds): Unit = {
    println(s"**** Sleeping ${duration.toSeconds} seconds")
    Thread.sleep(duration.toMillis)
  }
}