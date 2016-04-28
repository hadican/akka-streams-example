package com.hadican.akka.streams.example

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, Framing, FileIO}
import akka.util.ByteString
import com.hadican.akka.streams.example.model.Faq
import scala.concurrent.Await
import scala.concurrent.duration._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

object Main {

  def main(args: Array[String]) {

    // implicits
    implicit val actorSystem = ActorSystem("AkkaStreams")
    implicit val materializer = ActorMaterializer()
    implicit val formats = DefaultFormats

    // read file
    val jsonFile = new File("src/main/resources/mock-data.json")
    val jsonAsFuture = FileIO.fromFile(jsonFile).
      via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
      map(_.utf8String.trim).runFold("")((lines, line) => lines + line)
    val result: String = Await.result(jsonAsFuture, 1 second)

    // map to a model
    val faqs: List[Faq] = (parse(result) \ "faqs").extract[List[Faq]]

    // index faqs async and unordered way with parallelism level 2, and print out the response
    Source(faqs)
      .mapAsyncUnordered(2)(faq => Http().
        singleRequest(HttpRequest(method = HttpMethods.POST, uri = "http://localhost:9200/faqs/en", entity = write(faq))))
      .runForeach(println)
  }

}
