package com.example

import akka.actor.ActorSystem

// Running Elasticsearch:
// docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:6.4.2

object Main extends App {
  implicit val system = ActorSystem("Indexer")

  val numMessages: Int = 5
  val indexer = new MessageIndexer()

  def index: Unit = {
    for (i <- 1 to numMessages) {
      indexer.publish(Message(i, Haikunator.haiku))
    }
  }

  val idx = indexer.createIndex()
  println(s"Created initial index $idx")

  indexer.setAlias(idx, forRead = true, forWrite = true)
  println(s"Set read and write alias for $idx")

  index
  Thread.sleep(1000)
  println(s"Indexed data to $idx")

  /**
    * Imagine at this point we are doing a reindex.
    * We would need to either stop the flow of events from Zookeeper,
    * or buffer the input values temporarily.
    */
  val newIdx = indexer.createIndex()
  println(s"Created new index $newIdx")

  indexer.setAlias(newIdx, forWrite = true)
  println(s"Set write alias for $newIdx")

  index
  Thread.sleep(1000)
  println(s"Indexed data to $newIdx")

  indexer.setAlias(newIdx, forRead = true, forWrite = true)
  println(s"Set read and write alias for $newIdx")
}