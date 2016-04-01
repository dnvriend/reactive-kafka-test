package com.github.dnvriend.kafka

import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.scaladsl.Producer.Message
import akka.stream.scaladsl.Source
import com.github.dnvriend.TestSpec
import org.apache.kafka.clients.producer.ProducerRecord

class ProducerConsumerTest extends TestSpec {

  it should "produce and consume messages" in {
    Source(1 to 10000)
      .map(_.toString)
      .map(elem => Message(new ProducerRecord[Array[Byte], String]("topic1", elem), elem))
      .via(Producer.flow(producerSettings))
      .map { result =>
        val record = result.message.record
        println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value} (${result.message.passThrough})")
        result
      }
      .runForeach(_ => ()).toTry should be a 'success

    Consumer.plainSource(consumerSettings.withClientId("client1"))
      .take(1000)
      .runForeach(println).toTry should be a 'success
  }

}
