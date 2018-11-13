name := "akka-streams-indexer"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.18"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % "1.0-M1",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

