package com.example

import spray.json._

case class Message(id: Int, name: String) {
  override def toString: String = s"$id: $name"
}

object MessageJsonProtocol extends DefaultJsonProtocol {
  implicit val format: JsonFormat[Message] = jsonFormat2(Message.apply)
}