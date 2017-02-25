/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.benchmarks.Timed._
import akka.kafka.benchmarks.app.RunTestCommand
import akka.stream.Materializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.Future
import scala.language.postfixOps

object Benchmarks {

  type Key = Array[Byte]
  type Value = Array[Byte]
  type BenchmarkProducerRecord = ProducerRecord[Key, Value]
  type BenchmarkProducer = KafkaProducer[Key, Value]
  type BenchmarkConsumer = KafkaConsumer[Key, Value]
  type BenchmarkConsumerRecord = ConsumerRecord[Key, Value]
  type CommittableMsg = CommittableMessage[Key, Value]

  def run(cmd: RunTestCommand)(implicit actorSystem: ActorSystem, mat: Materializer): Unit = {

    cmd.testName match {
      case "plain-consumer-nokafka" =>
        runPerfTest(cmd, KafkaConsumerFixtures.noopFixtureGen(cmd), KafkaConsumerBenchmarks.consumePlainNoKafka)
      case "akka-plain-consumer-nokafka" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.noopFixtureGen(cmd), ReactiveKafkaConsumerBenchmarks.consumePlainNoKafka)
      case "plain-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumePlain)
      case "akka-plain-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.plainSources(cmd), ReactiveKafkaConsumerBenchmarks.consumePlain)
      case "batched-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
      case "akka-batched-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.commitableSources(cmd), ReactiveKafkaConsumerBenchmarks.consumerAtLeastOnceBatched(batchSize = 1000))
      case "at-most-once-consumer" =>
        runPerfTest(cmd, KafkaConsumerFixtures.filledTopics(cmd), KafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "akka-at-most-once-consumer" =>
        runPerfTest(cmd, ReactiveKafkaConsumerFixtures.commitableSources(cmd), ReactiveKafkaConsumerBenchmarks.consumeCommitAtMostOnce)
      case "plain-producer" =>
        runPerfTest(cmd, KafkaProducerFixtures.initializedProducer(cmd), KafkaProducerBenchmarks.plainFlow)
      case "akka-plain-producer" =>
        runPerfTest(cmd, ReactiveKafkaProducerFixtures.flowFixture(cmd), ReactiveKafkaProducerBenchmarks.plainFlow)
      case _ => Future.failed(new IllegalArgumentException(s"Unrecognized test name: ${cmd.testName}"))
    }
  }

}
