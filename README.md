# reactive-kafka-test
Small study project on [Apache Kafka](http://kafka.apache.org/) which is publish-subscribe messaging rethought as a distributed commit log. A good read is the book [Unified Log Processing by Alexander Dean](https://www.manning.com/books/unified-log-processing) and the ideas by [Martin Kleppmann's book: Designing Data-Intensive Applications](http://my.safaribooksonline.com/book/software-engineering-and-development/9781491903063), and has [some good talks on Youtube](https://www.youtube.com/channel/UClB4KPy5LkJj1t3SgYVtMOQ) so enjoy!

# Building and launching
This project builds `v0.9.0.1` of Kafka and uses Zookeeper `v3.4.5`. Just execute the `launch.sh` script and all will be build and launched.

# Hello World
The ["helloworld"](http://kafka.apache.org/documentation.html#quickstart_send) of zookeeper is the `console-producer` and `console-consumer` example, in which a producer places a message on the topic `test` and the consumer, consumes the message. In contrast to message brokers, the message will stay persisted after it has been processed by a consumer (the log concept) and will always be available to new consumers that start processing the topic `test` from `the beginning`.

# Youtube
- [Martin Kleppmann - Building real-time data products at LinkedIn with Apache Samza](https://www.youtube.com/watch?v=yO3SBU6vVKA)
- [Jay Kreps - Stream Processing with Apache Kafka](https://www.youtube.com/watch?v=Z9UrIm2Nc7s)
- [Jay Kreps - Apache Kafka and the Next 700 Stream Processing Systems](https://www.youtube.com/watch?v=9RMOc0SwRro)
- [Jay Kreps - I Love Logs: Apache Kafka and Real-Time Data Integration](https://www.youtube.com/watch?v=aJuo_bLSW6s)
- [Jay Kreps - Putting Apache Kafka to Use for Event Streams](https://www.youtube.com/watch?v=el-SqcZLZlI)
- [Joe Stein - Introduction to Apache Kafka](https://www.youtube.com/watch?v=qc33qMUvR7c)
- [Cody Koeninger - Exactly Once Streaming from Kafka](https://www.youtube.com/watch?v=fXnNEq1v3VA)

# Camel
- [Camel Kafka Component](http://camel.apache.org/kafka.html)

# more to come...
...

# resources
https://github.com/yahoo/kafka-manager
