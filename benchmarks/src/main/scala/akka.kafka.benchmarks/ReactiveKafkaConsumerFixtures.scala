/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.benchmarks.ReactiveKafkaConsumerBenchmarks.{CommitableFixture, NonCommitableFixture}
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
case class ReactiveKafkaConsumerTestFixture[T](topic: String, msgCount: Int, msgBytes: Int, source: Source[T, Control])

object ReactiveKafkaConsumerFixtures extends PerfFixtureHelpers {

  private def createConsumerSettings(kafkaHost: String)(implicit actorSystem: ActorSystem) =
    ConsumerSettings(actorSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafkaHost)
      .withGroupId(randomId())
      .withClientId(randomId())
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def plainSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[NonCommitableFixture](c, (msgCount, msgBytes) => {
      val topic = randomId()
      fillTopic(c.kafkaHost, topic, msgCount)
      val settings = createConsumerSettings(c.kafkaHost)
      val source = Consumer.plainSource(settings, Subscriptions.topics(topic))
      ReactiveKafkaConsumerTestFixture(topic, msgCount, msgBytes, source)
    })

  def commitableSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[CommitableFixture](c, (msgCount, msgBytes) => {
      val topic = randomId()
      fillTopic(c.kafkaHost, topic, msgCount)
      val settings = createConsumerSettings(c.kafkaHost)
      val source = Consumer.committableSource(settings, Subscriptions.topics(topic))
      ReactiveKafkaConsumerTestFixture(topic, msgCount, msgBytes, source)
    })

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[NonCommitableFixture](
    c, (msgCount, msgBytes) => {
    ReactiveKafkaConsumerTestFixture("topic", msgCount, msgBytes, null)
  }
  )

}
