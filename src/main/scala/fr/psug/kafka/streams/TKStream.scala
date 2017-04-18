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
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier, StreamPartitioner}

import scala.language.implicitConversions

/**
  * Typesafe wrapper for kafka's org.apache.kafka.streams.kstream.KStream
  *
  * @param source - wrapped stream
  * @tparam K - key
  * @tparam V - value
  */
class TKStream[K, V](val source: KStream[K, V]) {

  private implicit def streamToTypesafe[I, J](source: KStream[I, J]): TKStream[I, J] = new TKStream(source)

  private implicit def groupedStreamToTypesafe[I, J](source: KGroupedStream[I, J]): TKGroupedStream[I, J] = new TKGroupedStream(source)

  def filter(predicate: (K, V) => Boolean): TKStream[K, V] =
    source.filter(new Predicate[K, V] {
      override def test(key: K, value: V): Boolean = predicate(key, value)
    })

  def filterNot(predicate: (K, V) => Boolean): TKStream[K, V] =
    source.filterNot(new Predicate[K, V] {
      override def test(key: K, value: V): Boolean = predicate(key, value)
    })

  def selectKey[K1](mapper: (K, V) => K1): TKStream[K1, V] =
    source.selectKey(new KeyValueMapper[K, V, K1] {
      override def apply(key: K, value: V): K1 = {
        mapper(key, value)
      }
    })

  def map[K1, V1](mapper: (K, V) => (K1, V1)): TKStream[K1, V1] =
    source.map(new KeyValueMapper[K, V, KeyValue[K1, V1]] {
      override def apply(key: K, value: V): KeyValue[K1, V1] = {
        val (outK, outV) = mapper(key, value)
        new KeyValue[K1, V1](outK, outV)
      }
    })

  def mapValues[V1](mapper: V => V1): TKStream[K, V1] =
    source.mapValues(new ValueMapper[V, V1] {
      override def apply(value: V): V1 = mapper(value)
    })

  def print(keySerde: Serde[K], valSerde: Serde[V]): Unit = source.print(keySerde, valSerde)

  def writeAsText(filePath: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
    source.writeAsText(filePath, keySerde, valSerde)

  def flatMap[K1, V1](mapper: (K, V) => Iterable[(K1, V1)]): TKStream[K1, V1] =
    source.flatMap(new KeyValueMapper[K, V, java.lang.Iterable[KeyValue[K1, V1]]] {
      override def apply(key: K, value: V): java.lang.Iterable[KeyValue[K1, V1]] = {
        import scala.collection.JavaConverters._
        mapper(key, value).map { case (k, v) => new KeyValue[K1, V1](k, v) }.asJava
      }
    })

  def flatMapValues[V1](mapper: V => Iterable[V1]): TKStream[K, V1] =
    source.flatMapValues(new ValueMapper[V, java.lang.Iterable[V1]] {
      override def apply(value: V): java.lang.Iterable[V1] = {
        import scala.collection.JavaConverters._
        mapper(value).asJava
      }
    })

  def branch(predicates: ((K, V) => Boolean)*): Array[TKStream[K, V]] = {
    source
      .branch(predicates.map(p =>
        new Predicate[K, V]() {
          override def test(key: K, value: V): Boolean = p(key, value)
        }): _*)
      .map(x => x: TKStream[K, V])
  }

  /**
    * DOES NOT EXIST IN REAL LIFE
    *
    * @param predicate to segregate data
    * @return
    */
  def partition(predicate: (K, V) => Boolean): (TKStream[K, V], TKStream[K, V]) = {
    val in  = source.filter(predicate)
    val out = source.filterNot(predicate)
    (in, out)
  }

  def foreach(func: (K, V) => Unit): Unit =
    source.foreach(new ForeachAction[K, V] {
      override def apply(key: K, value: V): Unit = func(key, value)
    })

  def through(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): TKStream[K, V] =
    source.through(keySerde, valSerde, topic)

  def through(partitioner: StreamPartitioner[K, V], topic: String)(implicit keySerde: Serde[K],
                                                                   valSerde: Serde[V]): TKStream[K, V] =
    source.through(keySerde, valSerde, partitioner, topic)

  def to(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit = {
    source.to(keySerde, valSerde, topic)
  }

  def to(partitioner: StreamPartitioner[K, V], topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
    source.to(keySerde, valSerde, partitioner, topic)

  def transform[K1, V1](transformerSupplier: () => Transformer[K, V, KeyValue[K1, V1]],
                        stateStoreNames: String*): TKStream[K1, V1] =
    source.transform(new TransformerSupplier[K, V, KeyValue[K1, V1]] {
      override def get(): Transformer[K, V, KeyValue[K1, V1]] = transformerSupplier()
    }, stateStoreNames: _*)

  def transformValues[R](valueTransformerSupplier: () => ValueTransformer[V, R],
                         stateStoreNames: String*): TKStream[K, R] = {
    source.transformValues(new ValueTransformerSupplier[V, R] {
      override def get(): ValueTransformer[V, R] = valueTransformerSupplier()
    }, stateStoreNames: _*)
  }

  def process(processorSupplier: () => Processor[K, V], stateStoreNames: String*): Unit = {
    source.process(new ProcessorSupplier[K, V] {
      override def get(): Processor[K, V] = processorSupplier()
    }, stateStoreNames: _*)
  }

  def join[V1, R](otherStream: TKStream[K, V1], joiner: (V, V1) => R, windows: JoinWindows)(
    implicit keySerde: Serde[K],
    thisValueSerde: Serde[V],
    otherValueSerde: Serde[V1]): TKStream[K, R] = {
    source.join(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1): R = joiner(value1, value2)
    }, windows, keySerde, thisValueSerde, otherValueSerde)
  }

  def outerJoin[V1, R](otherStream: TKStream[K, V1], joiner: (V, V1) => R, windows: JoinWindows)(
    implicit keySerde: Serde[K],
    thisValueSerde: Serde[V],
    otherValueSerde: Serde[V1]): TKStream[K, R] = {
    source.outerJoin(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1): R = joiner(value1, value2)
    }, windows, keySerde, thisValueSerde, otherValueSerde)
  }

  def groupByKey(implicit keySerde: Serde[K], valSerde: Serde[V]): TKGroupedStream[K, V] = source.groupByKey(keySerde, valSerde)

  def groupBy[K1](keySelector: (K, V) => K1)(implicit keySerde: Serde[K1], valSerde: Serde[V]): TKGroupedStream[K1, V] = {
    source.groupBy(new KeyValueMapper[K, V, K1] {
      override def apply(key: K, value: V): K1 = keySelector(key, value)
    }, keySerde, valSerde)
  }

  def leftJoin[V1, R](otherStream: TKStream[K, V1], joiner: (V, V1) => R, windows: JoinWindows)(
    implicit keySerde: Serde[K], thisValueSerde: Serde[V],
    otherValueSerde: Serde[V1]): TKStream[K, R] =
    source.leftJoin(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1): R = joiner(value1, value2)
    }, windows, keySerde, thisValueSerde, otherValueSerde)

  def leftJoin[V1, V2](otherStream: TKStream[K, V1], joiner: (V, V1) => V2, windows: JoinWindows): TKStream[K, V2] =
    source.leftJoin(otherStream.source, new ValueJoiner[V, V1, V2] {
      override def apply(value1: V, value2: V1): V2 = joiner(value1, value2)
    }, windows)

  def leftJoin[V1, V2](table: KTable[K, V1], joiner: (V, V1) => V2): TKStream[K, V2] =
    source.leftJoin(table, new ValueJoiner[V, V1, V2] {
      override def apply(value1: V, value2: V1): V2 = joiner(value1, value2)
    })

  def leftJoin[V1, V2](table: KTable[K, V1], joiner: (V, V1) => V2)
                      (implicit keySerde: Serde[K], valSerde: Serde[V]): TKStream[K, V2] =
    source.leftJoin(table, new ValueJoiner[V, V1, V2] {
      override def apply(value1: V, value2: V1): V2 = joiner(value1, value2)
    }, keySerde, valSerde)

}
