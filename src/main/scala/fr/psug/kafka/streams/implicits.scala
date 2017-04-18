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

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KStream, KTable}

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
  implicit class TypesafeKStream[K, V](source: KStream[K, V]) {
    def typesafe = new TKStream[K, V](source)
  }

  implicit class TypesafeTKTable[K, V](source: KTable[K, V]) {
    def typesafe = new TKTable[K, V](source)
  }


  implicit  def typesafeKStreamUnapply[K, V](tKStream: TKStream[K, V]):KStream[K, V]  =  tKStream.source

  implicit  def typesafeKTableUnapply[K, V](tKSTable: TKTable[K, V]):KTable[K, V]  =  tKSTable.source
}