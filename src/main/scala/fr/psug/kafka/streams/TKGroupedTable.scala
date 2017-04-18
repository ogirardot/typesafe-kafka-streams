package fr.psug.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._

class TKGroupedTable[K, V](source: KGroupedTable[K, V]) {

  def aggregate[T](initializer: () => T, adder: (K, V, T) => T,subtractor: (K, V, T) => T, storeName: String)
                  (implicit aggValueSerde: Serde[T]): KTable[K, T] = {
    source.aggregate(new Initializer[T] {
      override def apply(): T = initializer()
    }, new Aggregator[K, V, T] {
      override def apply(aggKey: K, value: V, aggregate: T): T = adder(aggKey, value, aggregate)
    }, new Aggregator[K, V, T] {
      override def apply(aggKey: K, value: V, aggregate: T): T = subtractor(aggKey, value, aggregate)
    }, aggValueSerde, storeName)
  }


  def count(storeName: String): KTable[K, java.lang.Long] =
    source.count(storeName)


  def reduce(adder: (V, V) => V,subtractor: (V, V) => V, storeName: String): KTable[K, V] = {
    source.reduce(
      new Reducer[V] {
        override def apply(value1: V, value2: V): V = adder(value1, value2)
      }, new Reducer[V] {
        override def apply(value1: V, value2: V): V = subtractor(value1, value2)
      },
      storeName)
  }


}
