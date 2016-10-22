package com.github.ogirardot.kafka.streams

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.KeyValue

/**
  * Created by ogirardot on 22/10/2016.
  */
object KafkaStreamsImplicitSerde {

  implicit val stringSerde: Serde[String] = Serdes.String()

  implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()

  //implicit def genericAvroSerde(implicit schema: Schema): Serde[GenericRecord] = new GenericAvroSerde(schema)

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}
