/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.kafka.benchmarks.Benchmarks.{BenchmarkProducer, BenchmarkProducerRecord}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps

private[benchmarks] trait PerfFixtureHelpers extends LazyLogging {

  val producerTimeout = 6 minutes
  val logPercentStep = 1
  val Rnd = ThreadLocalRandom.current()

  def randomId() = UUID.randomUUID().toString

  def fillTopic(kafkaHost: String, topic: String, msgCount: Int): Unit = {
    val producer = initTopicAndProducer(kafkaHost, topic, msgCount)
    producer.close()
  }

  def randomByteArray(msgBytes: Int): Array[Byte] = {
    val array = new Array[Byte](msgBytes)
    Rnd.nextBytes(array)
    array
  }

  def initTopicAndProducer(kafkaHost: String, topic: String, msgCount: Int = 1, msgBytes: Int = 10): BenchmarkProducer = {
    val producerJavaProps = new java.util.Properties
    producerJavaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
    logger.info(s"Initializing producer connection to $kafkaHost")
    val producer = new BenchmarkProducer(producerJavaProps, new ByteArraySerializer, new ByteArraySerializer)
    val lastElementStoredPromise = Promise[Unit]
    val loggedStep = if (msgCount > logPercentStep) msgCount / (100 / logPercentStep) else 1
    logger.info(s"Producing messages to topic $topic")
    for (i <- 0L to msgCount.toLong) {
      if (!lastElementStoredPromise.isCompleted) {
        producer.send(new BenchmarkProducerRecord(topic, randomByteArray(msgBytes)), new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            if (e == null) {
              if (i % loggedStep == 0)
                logger.info(s"Written $i elements to Kafka (${100 * i / msgCount}%)")
              if (recordMetadata.offset() == msgCount - 1 && !lastElementStoredPromise.isCompleted)
                lastElementStoredPromise.success(())
            }
            else {
              if (!lastElementStoredPromise.isCompleted) {
                e.printStackTrace()
                lastElementStoredPromise.failure(e)
              }
            }
          }
        })
      }
    }
    val lastElementStoredFuture = lastElementStoredPromise.future
    Await.result(lastElementStoredFuture, atMost = producerTimeout)
    producer
  }
}
