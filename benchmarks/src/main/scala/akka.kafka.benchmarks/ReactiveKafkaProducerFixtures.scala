/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.{Message, Result}
import akka.kafka.ProducerSettings
import akka.kafka.benchmarks.Benchmarks.{Key, Value}
import akka.kafka.benchmarks.ReactiveKafkaConsumerBenchmarks.NonCommitableFixture
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Flow
import org.apache.kafka.common.serialization.ByteArraySerializer

object ReactiveKafkaProducerFixtures extends PerfFixtureHelpers {

  val Parallelism = 100

  type In[PassThrough] = Message[Key, Value, PassThrough]
  type Out[PassThrough] = Result[Key, Value, PassThrough]
  type FlowType[PassThrough] = Flow[In[PassThrough], Out[PassThrough], NotUsed]

  case class ReactiveKafkaProducerTestFixture[PassThrough](topic: String, msgCount: Int, msgBytes: Int, flow: FlowType[PassThrough])

  private def createProducerSettings(kafkaHost: String)(implicit actorSystem: ActorSystem): ProducerSettings[Key, Value] =
    ProducerSettings(actorSystem, new ByteArraySerializer, new ByteArraySerializer)
      .withBootstrapServers(kafkaHost)
      .withParallelism(Parallelism)

  def flowFixture(c: RunTestCommand)(implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaProducerTestFixture[Int]](c, (msgCount, msgBytes) => {
    val flow: FlowType[Int] = Producer.flow(createProducerSettings(c.kafkaHost))
    val topic = randomId()
    initTopicAndProducer(c.kafkaHost, topic)
    ReactiveKafkaProducerTestFixture(topic, msgCount, msgBytes, flow)
  })

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[NonCommitableFixture](
    c, (msgCount, msgBytes) => {
    ReactiveKafkaConsumerTestFixture("topic", msgCount, msgBytes, null)
  }
  )

}
