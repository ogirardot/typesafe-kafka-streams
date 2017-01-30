package fr.psug.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._

class TKGroupedStream[K, V](source: KGroupedStream[K, V]) {

  def aggregate[T](initializer: () => T, aggregator: (K, V, T) => T, storeName: String)(
    aggValueSerde: Serde[T]): KTable[K, T] = {
    source.aggregate(new Initializer[T] {
      override def apply(): T = initializer()
    }, new Aggregator[K, V, T] {
      override def apply(aggKey: K, value: V, aggregate: T): T = aggregator(aggKey, value, aggregate)
    }, aggValueSerde, storeName)
  }

  def aggregate[T, W <: Window](initializer: () => T, aggregator: (K, V, T) => T, windows: Windows[W], storeName: String)(
    aggValueSerde: Serde[T]): KTable[Windowed[K], T] = {
    source.aggregate(new Initializer[T] {
      override def apply(): T = initializer()
    }, new Aggregator[K, V, T] {
      override def apply(aggKey: K, value: V, aggregate: T): T = aggregator(aggKey, value, aggregate)
    }, windows, aggValueSerde, storeName)
  }

  def count(storeName: String): KTable[K, java.lang.Long] =
    source.count(storeName)

  def count[W <: Window](windows: Windows[W], storeName: String): KTable[Windowed[K], java.lang.Long] =
    source.count(windows, storeName)

  def reduce(reducer: (V, V) => V, storeName: String): KTable[K, V] = {
    source.reduce(new Reducer[V] {
      override def apply(value1: V, value2: V): V = reducer(value1, value2)
    }, storeName)
  }

  def reduce[W <: Window](reducer: (V, V) => V, windows: Windows[W], storeName: String): KTable[Windowed[K], V] = {
    source.reduce(new Reducer[V] {
      override def apply(value1: V, value2: V): V = reducer(value1, value2)
    }, windows, storeName)
  }
}
