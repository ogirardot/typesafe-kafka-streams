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

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.StreamPartitioner

import scala.language.implicitConversions

/**
  * Typesafe wrapper for kafka's org.apache.kafka.streams.kstream.KStream
  *
  * @param source - wrapped stream
  * @tparam K - key
  * @tparam V - value
  */
class TKTable[K, V](val source: KTable[K, V]) {

  import TKTable._

  def filter(predicate: (K, V) => Boolean): TKTable[K, V] =
    source.filter(new Predicate[K, V] {
      override def test(key: K, value: V): Boolean = predicate(key, value)
    })

  def filterNot(predicate: (K, V) => Boolean): TKTable[K, V] =
    source.filterNot(new Predicate[K, V] {
      override def test(key: K, value: V): Boolean = predicate(key, value)
    })


  def mapValues[V1](mapper: V => V1): TKTable[K, V1] =
    tableToTypesafe(source.mapValues(new ValueMapper[V, V1] {
      override def apply(value: V): V1 = mapper(value)
    }))

  def print(keySerde: Serde[K], valSerde: Serde[V]): Unit = source.print(keySerde, valSerde)

  def writeAsText(filePath: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
    source.writeAsText(filePath, keySerde, valSerde)


  /**
    * DOES NOT EXIST IN REAL LIFE
    *
    * @param predicate to segregate data
    * @return
    */
  def partition(predicate: (K, V) => Boolean): (TKTable[K, V], TKTable[K, V]) = {
    val in = source.filter(predicate)
    val out = source.filterNot(predicate)
    (in, out)
  }

  def foreach(func: (K, V) => Unit): Unit =
    source.foreach(new ForeachAction[K, V] {
      override def apply(key: K, value: V): Unit = func(key, value)
    })

  def through(topic: String, storeName: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): TKTable[K, V] =
    source.through(keySerde, valSerde, topic, storeName)

  def through(partitioner: StreamPartitioner[K, V], topic: String, storeName: String)(implicit keySerde: Serde[K],
                                                                                      valSerde: Serde[V]): TKTable[K, V] =
    source.through(keySerde, valSerde, partitioner, topic, storeName)

  def to(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit = {
    source.to(keySerde, valSerde, topic)
  }

  def to(partitioner: StreamPartitioner[K, V], topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
    source.to(keySerde, valSerde, partitioner, topic)


  def join[V1, R](otherStream: TKTable[K, V1], joiner: (V, V1) => R)(implicit keySerde: Serde[K], valSerde: Serde[V1]): TKTable[K, R] = {
    tableToTypesafe(source.join(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1): R = joiner(value1, value2)
    }))
  }


  def outerJoin[V1, R](otherStream: TKTable[K, V1], joiner: (V, V1) => R)(implicit keySerde: Serde[K], valSerde: Serde[V1]): TKTable[K, R] = {
    tableToTypesafe(source.outerJoin(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1): R = joiner(value1, value2)
    }))
  }

  def groupBy[K1, V1](selector: (K, V) => KeyValue[K1, V1])(implicit keySerde: Serde[K1], valSerde: Serde[V1]): TKGroupedTable[K1, V1] = {
    source.groupBy(new KeyValueMapper[K, V, KeyValue[K1, V1]] {
      override def apply(key: K, value: V): KeyValue[K1, V1] = selector(key, value)
    }, keySerde, valSerde)
  }


  def leftJoin[V1, R](otherStream: TKTable[K, V1], joiner: (V, V1) => R)(implicit keySerde: Serde[K], valSerde: Serde[V1]): TKTable[K, R] =
    tableToTypesafe(source.leftJoin(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1): R = joiner(value1, value2)
    }))


  def getStoreName: String = source.getStoreName

  def toStream: TKStream[K, V] = new TKStream[K, V](source.toStream)

}

object TKTable {
  private implicit def tableToTypesafe[I, J](source: KTable[I, J]): TKTable[I, J] = new TKTable(source)

  private implicit def groupedStreamToTypesafe[I, J](source: KGroupedTable[I, J]): TKGroupedTable[I, J] = new TKGroupedTable(source)

}