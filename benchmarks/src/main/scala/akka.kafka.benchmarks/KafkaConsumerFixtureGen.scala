/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.benchmarks.Benchmarks.BenchmarkConsumer
import akka.kafka.benchmarks.app.RunTestCommand
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConversions._

case class KafkaConsumerTestFixture(
    topic: String,
    msgCount: Int,
    msgBytes: Int,
    consumer: BenchmarkConsumer
) {
  def close(): Unit = consumer.close()
}

object KafkaConsumerFixtures extends PerfFixtureHelpers {

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[KafkaConsumerTestFixture](
    c, (msgCount, msgBytes) => {
    KafkaConsumerTestFixture("topic", msgCount, msgBytes, null)
  }
  )

  def filledTopics(c: RunTestCommand) = FixtureGen[KafkaConsumerTestFixture](
    c, (msgCount, msgBytes) => {
    val topic = randomId()
    fillTopic(c.kafkaHost, topic, msgCount)
    val consumerJavaProps = new java.util.Properties
    consumerJavaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, c.kafkaHost)
    consumerJavaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, randomId())
    consumerJavaProps.put(ConsumerConfig.GROUP_ID_CONFIG, randomId())
    consumerJavaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new BenchmarkConsumer(consumerJavaProps, new ByteArrayDeserializer, new ByteArrayDeserializer)
    consumer.subscribe(Set(topic))
    KafkaConsumerTestFixture(topic, msgCount, msgBytes, consumer)
  }
  )
}
