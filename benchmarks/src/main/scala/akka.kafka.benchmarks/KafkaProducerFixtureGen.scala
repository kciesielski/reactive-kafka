/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.benchmarks.Benchmarks.BenchmarkProducer
import akka.kafka.benchmarks.app.RunTestCommand

case class KafkaProducerTestFixture(
    topic: String,
    msgCount: Int,
    msgBytes: Int,
    producer: BenchmarkProducer
) {
  def close(): Unit = producer.close()
}

object KafkaProducerFixtures extends PerfFixtureHelpers {

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[KafkaProducerTestFixture](
    c, (msgCount, msgBytes) => {
    KafkaProducerTestFixture("topic", msgCount, msgBytes, null)
  }
  )

  def initializedProducer(c: RunTestCommand) = FixtureGen[KafkaProducerTestFixture](
    c, (msgCount, msgBytes) => {
    val topic = randomId()
    val rawProducer = initTopicAndProducer(c.kafkaHost, topic)
    KafkaProducerTestFixture(topic, msgCount, msgBytes, rawProducer)
  }
  )
}
