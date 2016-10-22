package com.github.ogirardot.kafka.streams

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream

import scala.language.implicitConversions

object KafkaStreamsImplicitSerdes {

  implicit val stringSerde: Serde[String] = Serdes.String()

  implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()

  //implicit def genericAvroSerde(implicit schema: Schema): Serde[GenericRecord] = new GenericAvroSerde(schema)

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}

object KafkaStreamsImplicits {

  /**
    * Enter the world of a scala-friendly KStream api
    *
    * @param source kstream to wrap
    * @tparam K key type
    * @tparam V value type
    */
  implicit class TypesafeImprovement[K, V](source: KStream[K, V]) {
    def typesafe = new TKStream[K, V](source)
  }

}