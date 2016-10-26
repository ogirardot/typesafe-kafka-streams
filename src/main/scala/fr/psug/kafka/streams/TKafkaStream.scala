/*
 * Copyright (c) 2016 Fred Cecilia, Valentin Kasas, Olivier Girardot
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fr.psug.kafka.streams

import java.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory
import KafkaStreamsImplicits._

/**
  * Trait that will give you all the perks of TKStreams
  * and simple bootstrapping
  */
trait TKafkaStream[K, V] {

  val log = LoggerFactory.getLogger(getClass)

  def inputTopic: String

  def applicationId: String

  def brokersURI: String

  def zookeeperURI: String

  lazy val builder: KStreamBuilder = new KStreamBuilder()

  def source(inputTopic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): TKStream[K, V] =
    builder.stream(keySerde, valSerde, inputTopic).typesafe

  def start(props: Properties = new Properties()) = {

    log.info(s"StreamsConfig.APPLICATION_ID_CONFIG : $applicationId")
    log.info(s"StreamsConfig.BOOTSTRAP_SERVERS_CONFIG  : $brokersURI")
    log.info(s"StreamsConfig.ZOOKEEPER_CONNECT_CONFIG : $zookeeperURI")

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURI)
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperURI)

    val streams: KafkaStreams = new KafkaStreams(builder, props)
    streams.start()
    streams
  }
}
