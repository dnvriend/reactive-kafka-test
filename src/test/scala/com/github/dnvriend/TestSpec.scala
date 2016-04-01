package com.github.dnvriend

import akka.NotUsed
import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait TestSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val pc: PatienceConfig = PatienceConfig(timeout = 50.seconds)

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("boot2docker:9092")

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer,
    Set("topic1"))
    .withBootstrapServers("boot2docker:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  implicit class FutureToTry[T](f: Future[T]) {
    def toTry: Try[T] = Try(f.futureValue)
  }

  /**
    * Returns a Source[Int, Unit]
    */
  def withIterator[T](start: Int = 0)(f: Source[Int, NotUsed] ⇒ T): T =
    f(Source.fromIterator(() ⇒ Iterator from start))

  override protected def afterAll(): Unit = {
    system.terminate()
    system.whenTerminated.toTry should be a 'success
  }
}