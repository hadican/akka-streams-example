name := "akka-streams-example"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.4",
  "com.typesafe.akka" %% "akka-http-core" % "2.4.4",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.4",
  "org.json4s" %% "json4s-jackson" % "3.3.0"

)