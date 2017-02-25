/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import java.util.concurrent.ThreadLocalRandom

import akka.kafka.benchmarks.Benchmarks.{BenchmarkProducerRecord, Value}
import com.codahale.metrics.Meter
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._

object KafkaProducerBenchmarks extends LazyLogging {

  val Rnd = ThreadLocalRandom.current()

  def randomMsg(msgBytes: Int): Value = {
    val array = new Array[Byte](msgBytes)
    Rnd.nextBytes(array)
    array
  }

  val logStep = 100000
  /**
   * Streams generated numbers to a Kafka producer. Does not commit.
   */
  def plainFlow(fixture: KafkaProducerTestFixture, meter: Meter): Unit = {
    val producer = fixture.producer
    var lastPartStart = System.nanoTime()

    for (i <- 1 to fixture.msgCount) {
      producer.send(new BenchmarkProducerRecord(fixture.topic, randomMsg(fixture.msgBytes)))
      meter.mark()
      if (i % logStep == 0) {
        val lastPartEnd = System.nanoTime()
        val took = (lastPartEnd - lastPartStart).nanos
        logger.info(s"Sent $i, took ${took.toMillis} ms to send last $logStep")
        lastPartStart = lastPartEnd
      }
    }
    fixture.close()
  }

}
