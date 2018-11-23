package com.example

import spray.json._

case class Message(id: Int, name: String, version: Long = 1) {
  override def toString: String = s"$id: $name (v$version)"
}

object MessageJsonProtocol extends DefaultJsonProtocol {
  implicit val format: JsonFormat[Message] = jsonFormat3(Message.apply)
}