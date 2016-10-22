package com.github.ogirardot.kafka.streams

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
