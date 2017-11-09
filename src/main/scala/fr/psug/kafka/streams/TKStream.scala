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

import java.lang

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KeyValueMapper, _}
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

  import TKStream._

  /**
    * Create a new {@code KStream} that consists of all records of this stream which satisfy the given predicate.
    * All records that do not satisfy the predicate are dropped.
    * This is a stateless record-by-record operation.
    *
    * @param predicate a filter { @link Predicate} that is applied to each record
    * @return a { @code KStream} that contains only those records that satisfy the given predicate
    * @see #filterNot(Predicate)
    */
  def filter(predicate: (K, V) => Boolean): TKStream[K, V] =
    source.filter(new Predicate[K, V] {
      override def test(key: K, value: V) = predicate(key, value)
    })

  /**
    * Create a new {@code KStream} that consists all records of this stream which do <em>not</em> satisfy the given
    * predicate.
    * All records that <em>do</em> satisfy the predicate are dropped.
    * This is a stateless record-by-record operation.
    *
    * @param predicate a filter { @link Predicate} that is applied to each record
    * @return a { @code KStream} that contains only those records that do <em>not</em> satisfy the given predicate
    * @see #filter(Predicate)
    */
  def filterNot(predicate: (K, V) => Boolean): TKStream[K, V] =
    source.filterNot(new Predicate[K, V] {
      override def test(key: K, value: V) = predicate(key, value)
    })

  /**
    * Set a new key (with possibly new type) for each input record.
    * The provided {@link KeyValueMapper} is applied to each input record and computes a new key for it.
    * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V>}.
    * This is a stateless record-by-record operation.
    * <p>
    * For example, you can use this transformation to set a key for a key-less input record {@code <null,V>} by
    * extracting a key from the value within your {@link KeyValueMapper}. The example below computes the new key as the
    * length of the value string.
    * <pre>{@code
    * KStream<Byte[], String> keyLessStream = builder.stream("key-less-topic");
     * KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer> {
     *     Integer apply(Byte[] key, String value) {
     *         return value.length();
     *     }
    * });
    * }</pre>
    * <p>
    * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
    * join) is applied to the result {@code KStream}.
    *
    * @param mapper a { @link KeyValueMapper} that computes a new key for each record
    * @tparam K1 the new key type of the result stream
    * @return a { @code KStream} that contains records with new key (possibly of different type) and unmodified value
    * @see #map(KeyValueMapper)
    * @see #flatMap(KeyValueMapper)
    * @see #mapValues(ValueMapper)
    * @see #flatMapValues(ValueMapper)
    */
  def selectKey[KK >: K, VV >: V, K1](mapper: (KK, VV) => K1): KStream[K1, V] =
    source.selectKey(new KeyValueMapper[KK, VV, K1] {
      override def apply(key: KK, value: VV) = mapper(key, value)
    })


  def map[KR, VR](mapper: (K, V) => (KR, VR)): TKStream[KR, VR] =
    streamToTypesafe(source.map(new KeyValueMapper[K, V, KeyValue[KR, VR]] {
      override def apply(key: K, value: V): KeyValue[KR, VR] = {
        val (outK, outV) = mapper(key, value)
        new KeyValue(outK, outV)
      }
    }))


  def mapValues[VR](mapper: V => VR): TKStream[K, VR] =
    new TKStream(source.mapValues(new ValueMapper[V, VR] {
      override def apply(value: V): VR = mapper(value)
    }))

  def print(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit = source.print(keySerde, valSerde)

  def writeAsText(filePath: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
    source.writeAsText(filePath, keySerde, valSerde)

  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): TKStream[KR, VR] =
    streamToTypesafe(source.flatMap(new KeyValueMapper[K, V, lang.Iterable[KeyValue[KR, VR]]] {
      override def apply(key: K, value: V): lang.Iterable[KeyValue[KR, VR]] = {

        import scala.collection.JavaConverters._
        mapper(key, value).map { case (k, v) => new KeyValue(k, v) }.asJava
      }
    }))

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
    val in = source.filter(predicate)
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


  def transformValues[R](valueTransformerSupplier: => ValueTransformer[V, R],
                         stateStoreNames: String*): TKStream[K, R] =
    streamToTypesafe(source.transformValues(new ValueTransformerSupplier[V, R] {
      override def get(): ValueTransformer[V, R] = valueTransformerSupplier
    }, stateStoreNames: _*))

  def process(processorSupplier: () => Processor[K, V], stateStoreNames: String*): Unit = {
    source.process(new ProcessorSupplier[K, V] {
      override def get(): Processor[K, V] = processorSupplier()
    }, stateStoreNames: _*)
  }

  /**
    * Group the records by their current key into a {@link KGroupedStream} while preserving the original values.
    * Grouping a stream on the record key is required before an aggregation operator can be applied to the data
    * (cf. {@link KGroupedStream}).
    * If a record key is {@code null} the record will not be included in the resulting {@link KGroupedStream}.
    * <p>
    * If a key changing operator was used before this operation (e.g., {@link #selectKey(KeyValueMapper)},
    * {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)}, or
    * {@link #transform(TransformerSupplier, String...)}), and no data redistribution happened afterwards (e.g., via
    * {@link #through(String)}) an internal repartitioning topic will be created in Kafka.
    * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
    * {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
    * an internally generated name, and "-repartition" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    * <p>
    * For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
    * records to it, and rereading all records from it, such that the resulting {@link KGroupedStream} is partitioned
    * correctly on its key.
    *
    * @param keySerde key serdes for materializing this stream,
    *                 if not specified the default serdes defined in the configs will be used
    * @param valSerde value serdes for materializing this stream,
    *                 if not specified the default serdes defined in the configs will be used
    * @return a { @link KGroupedStream} that contains the grouped records of the original { @code KStream}
    */
  def groupByKey(implicit keySerde: Serde[K], valSerde: Serde[V]): TKGroupedStream[K, V] = source.groupByKey(keySerde, valSerde)


  /**
    * Group the records of this {@code KStream} on a new key that is selected using the provided {@link KeyValueMapper}
    * and default serializers and deserializers.
    * Grouping a stream on the record key is required before an aggregation operator can be applied to the data
    * (cf. {@link KGroupedStream}).
    * The {@link KeyValueMapper} selects a new key (with should be of the same type) while preserving the original values.
    * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedStream}
    * <p>
    * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
    * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
    * {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
    * an internally generated name, and "-repartition" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    * <p>
    * All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
    * and rereading all records from it, such that the resulting {@link KGroupedStream} is partitioned on the new key.
    * <p>
    * This operation is equivalent to calling {@link #selectKey(KeyValueMapper)} followed by {@link #groupByKey()}.
    * If the key type is changed, it is recommended to use {@link #groupBy(KeyValueMapper, Serde, Serde)} instead.
    *
    * @param keySelector a { @link KeyValueMapper} that computes a new key for grouping
    * @tparam K1 the key type of the result { @link KGroupedStream}
    * @return a { @link KGroupedStream} that contains the grouped records of the original { @code KStream}
    */
  def groupBy[K1](keySelector: (K, V) => K1)(implicit keySerde: Serde[K1], valSerde: Serde[V]): TKGroupedStream[K1, V] = source.groupBy(new KeyValueMapper[K, V, K1] {
    override def apply(key: K, value: V) = keySelector(key, value)
  }, keySerde, valSerde)

  /**
    * Join records of this stream with another {@code KStream}'s records using windowed inner equi join.
    * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
    * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
    * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
    * <p>
    * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
    * a value (with arbitrary type) for the result record.
    * The key of the result record is the same as for both joining input records.
    * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
    * output record will be added to the resulting {@code KStream}.
    * <p>
    * Example (assuming all input records belong to the correct windows):
    * <table border='1'>
    * <tr>
    * <th>this</th>
    * <th>other</th>
    * <th>result</th>
    * </tr>
    * <tr>
    * <td>&lt;K1:A&gt;</td>
    * <td></td>
    * <td></td>
    * </tr>
    * <tr>
    * <td>&lt;K2:B&gt;</td>
    * <td>&lt;K2:b&gt;</td>
    * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
    * </tr>
    * <tr>
    * <td></td>
    * <td>&lt;K3:c&gt;</td>
    * <td></td>
    * </tr>
    * </table>
    * Both input streams need to be co-partitioned on the join key.
    * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
    * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
    * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
    * user-specified in {@link StreamsConfig} via parameter
    * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
    * "-repartition" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    * <p>
    * Repartitioning can happen for one or both of the joining {@code KStream}s.
    * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
    * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
    * correctly on its key.
    * <p>
    * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
    * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
    * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
    * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
    * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    *
    * @param otherStream     the { @code KStream} to be joined with this stream
    * @param joiner          a { @link ValueJoiner} that computes the join result for a pair of matching records
    * @param windows         the specification of the { @link JoinWindows}
    * @param keySerde        key serdes for materializing both streams,
    *                        if not specified the default serdes defined in the configs will be used
    * @param thisValueSerde  value serdes for materializing this stream,
    *                        if not specified the default serdes defined in the configs will be used
    * @param otherValueSerde value serdes for materializing the other stream,
    *                        if not specified the default serdes defined in the configs will be used
    * @tparam VO the value type of the other stream
    * @tparam VR the value type of the result stream
    * @return a { @code KStream} that contains join-records for each key and values computed by the given
    *         { @link ValueJoiner}, one for each matched record-pair with the same key and within the joining window intervals
    * @see #leftJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
    * @see #outerJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
    */
  def join[VO, VR](otherStream: TKStream[K, VO],
                   joiner: (V, VO) => VR,
                   windows: JoinWindows)(
                    implicit keySerde: Serde[K],
                    thisValueSerde: Serde[V],
                    otherValueSerde: Serde[VO]): TKStream[K, VR] = streamToTypesafe(source.join(otherStream.source, new ValueJoiner[V, VO, VR] {
    override def apply(value1: V, value2: VO) = joiner(value1, value2)
  }, windows, keySerde, thisValueSerde, otherValueSerde))


  /**
    * Join records of this stream with {@link KTable}'s records using non-windowed inner equi join with default
    * serializers and deserializers.
    * The join is a primary key table lookup join with join attribute {@code stream.key == table.key}.
    * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
    * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
    * {@link KTable} state.
    * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
    * will not produce any result records.
    * <p>
    * For each {@code KStream} record that finds a corresponding record in {@link KTable} the provided
    * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
    * The key of the result record is the same as for both joining input records.
    * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
    * operation and thus no output record will be added to the resulting {@code KStream}.
    * <p>
    * Example:
    * <table border='1'>
    * <tr>
    * <th>KStream</th>
    * <th>KTable</th>
    * <th>state</th>
    * <th>result</th>
    * </tr>
    * <tr>
    * <td>&lt;K1:A&gt;</td>
    * <td></td>
    * <td></td>
    * <td></td>
    * </tr>
    * <tr>
    * <td></td>
    * <td>&lt;K1:b&gt;</td>
    * <td>&lt;K1:b&gt;</td>
    * <td></td>
    * </tr>
    * <tr>
    * <td>&lt;K1:C&gt;</td>
    * <td></td>
    * <td>&lt;K1:b&gt;</td>
    * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
    * </tr>
    * </table>
    * Both input streams need to be co-partitioned on the join key (cf.
    * {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner)}).
    * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
    * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
    * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
    * user-specified in {@link StreamsConfig} via parameter
    * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
    * "-repartition" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    * <p>
    * Repartitioning can happen only for this {@code KStream}s.
    * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
    * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
    * correctly on its key.
    *
    * @param table  the { @link KTable} to be joined with this stream
    * @param joiner a { @link ValueJoiner} that computes the join result for a pair of matching records
    * @tparam VT the value type of the table
    * @tparam VR the value type of the result stream
    * @return a { @code KStream} that contains join-records for each key and values computed by the given
    *         { @link ValueJoiner}, one for each matched record-pair with the same key
    * @see #leftJoin(KTable, ValueJoiner)
    * @see #join(GlobalKTable, KeyValueMapper, ValueJoiner)
    */
  def join[VT, VR](table: TKTable[K, VT], joiner: (V, VT) => VR): TKStream[K, VR] = {
    streamToTypesafe(source.join(table.source, new ValueJoiner[V, VT, VR] {
      override def apply(value1: V, value2: VT) = joiner(value1, value2)
    }))
  }


  /**
    * Join records of this stream with {@link GlobalKTable}'s records using non-windowed inner equi join.
    * The join is a primary key table lookup join with join attribute
    * {@code keyValueMapper.map(stream.keyValue) == table.key}.
    * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
    * This is done by performing a lookup for matching records in the <em>current</em> internal {@link GlobalKTable}
    * state.
    * In contrast, processing {@link GlobalKTable} input records will only update the internal {@link GlobalKTable}
    * state and will not produce any result records.
    * <p>
    * For each {@code KStream} record that finds a corresponding record in {@link GlobalKTable} the provided
    * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
    * The key of the result record is the same as the key of this {@code KStream}.
    * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
    * operation and thus no output record will be added to the resulting {@code KStream}.
    *
    * @param globalKTable   the { @link GlobalKTable} to be joined with this stream
    * @param keyValueMapper instance of { @link KeyValueMapper} used to map from the (key, value) of this stream
    *                       to the key of the { @link GlobalKTable}
    * @param joiner         a { @link ValueJoiner} that computes the join result for a pair of matching records
    * @tparam GK the key type of { @link GlobalKTable}
    * @tparam GV the value type of the { @link GlobalKTable}
    * @tparam RV the value type of the resulting { @code KStream}
    * @return a { @code KStream} that contains join-records for each key and values computed by the given
    *         { @link ValueJoiner}, one output for each input { @code KStream} record
    * @see #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
    */
  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
                       keyValueMapper: (K, V) => GK,
                       joiner: (V, GV) => RV): TKStream[K, RV] = streamToTypesafe(source.join(globalKTable,
    new KeyValueMapper[K, V, GK] {
      override def apply(key: K, value: V) = keyValueMapper(key, value)

    },
    new ValueJoiner[V, GV, RV] {
      override def apply(value1: V, value2: GV) = joiner(value1, value2)
    }))

  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
                       keyValueMapper: (K, V) => GK,
                       joiner: (V, GV) => RV): TKStream[K, RV] = streamToTypesafe(source.leftJoin(globalKTable,
    new KeyValueMapper[K, V, GK] {
      override def apply(key: K, value: V) = keyValueMapper(key, value)

    },
    new ValueJoiner[V, GV, RV] {
      override def apply(value1: V, value2: GV) = joiner(value1, value2)
    }))

  /**
    * Join records of this stream with another {@code KStream}'s records using windowed left equi join with default
    * serializers and deserializers.
    * In contrast to {@link #join(KStream, ValueJoiner, JoinWindows) inner-join}, all records from this stream will
    * produce at least one output record (cf. below).
    * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
    * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
    * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
    * <p>
    * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
    * a value (with arbitrary type) for the result record.
    * The key of the result record is the same as for both joining input records.
    * Furthermore, for each input record of this {@code KStream} that does not satisfy the join predicate the provided
    * {@link ValueJoiner} will be called with a {@code null} value for the other stream.
    * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
    * output record will be added to the resulting {@code KStream}.
    * <p>
    * Example (assuming all input records belong to the correct windows):
    * <table border='1'>
    * <tr>
    * <th>this</th>
    * <th>other</th>
    * <th>result</th>
    * </tr>
    * <tr>
    * <td>&lt;K1:A&gt;</td>
    * <td></td>
    * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
    * </tr>
    * <tr>
    * <td>&lt;K2:B&gt;</td>
    * <td>&lt;K2:b&gt;</td>
    * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
    * </tr>
    * <tr>
    * <td></td>
    * <td>&lt;K3:c&gt;</td>
    * <td></td>
    * </tr>
    * </table>
    * Both input streams need to be co-partitioned on the join key.
    * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
    * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
    * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
    * user-specified in {@link StreamsConfig} via parameter
    * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
    * "-repartition" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    * <p>
    * Repartitioning can happen for one or both of the joining {@code KStream}s.
    * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
    * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
    * correctly on its key.
    * <p>
    * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
    * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
    * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
    * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
    * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    *
    * @param otherStream the { @code KStream} to be joined with this stream
    * @param joiner      a { @link ValueJoiner} that computes the join result for a pair of matching records
    * @param windows     the specification of the { @link JoinWindows}
    * @tparam V1 the value type of the other stream
    * @tparam R  the value type of the result stream
    * @return a { @code KStream} that contains join-records for each key and values computed by the given
    *         { @link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
    *         this { @code KStream} and within the joining window intervals
    * @see #join(KStream, ValueJoiner, JoinWindows)
    * @see #outerJoin(KStream, ValueJoiner, JoinWindows)
    */
  def leftJoin[V1, R](otherStream: TKStream[K, V1],
                      joiner: (V, V1) => R,
                      windows: JoinWindows)(implicit keySerde: Serde[K], thisValueSerde: Serde[V],
                                            otherValueSerde: Serde[V1]): TKStream[K, R] =
    streamToTypesafe(source.leftJoin(otherStream.source, new ValueJoiner[V, V1, R] {
      override def apply(value1: V, value2: V1) = joiner(value1, value2)
    }, windows, keySerde, thisValueSerde, otherValueSerde))


  def leftJoin[V1, V2](table: KTable[K, V1],
                       joiner: (V, V1) => V2)
                      (implicit keySerde: Serde[K], valSerde: Serde[V]): TKStream[K, V2] =
    streamToTypesafe(source.leftJoin(table, new ValueJoiner[V, V1, V2] {
      override def apply(value1: V, value2: V1): V2 = joiner(value1, value2)
    }, keySerde, valSerde))

  /**
    * Join records of this stream with another {@code KStream}'s records using windowed left equi join with default
    * serializers and deserializers.
    * In contrast to {@link #join(KStream, ValueJoiner, JoinWindows) inner-join} or
    * {@link #leftJoin(KStream, ValueJoiner, JoinWindows) left-join}, all records from both streams will produce at
    * least one output record (cf. below).
    * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
    * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
    * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
    * <p>
    * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
    * a value (with arbitrary type) for the result record.
    * The key of the result record is the same as for both joining input records.
    * Furthermore, for each input record of both {@code KStream}s that does not satisfy the join predicate the provided
    * {@link ValueJoiner} will be called with a {@code null} value for the this/other stream, respectively.
    * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
    * output record will be added to the resulting {@code KStream}.
    * <p>
    * Example (assuming all input records belong to the correct windows):
    * <table border='1'>
    * <tr>
    * <th>this</th>
    * <th>other</th>
    * <th>result</th>
    * </tr>
    * <tr>
    * <td>&lt;K1:A&gt;</td>
    * <td></td>
    * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
    * </tr>
    * <tr>
    * <td>&lt;K2:B&gt;</td>
    * <td>&lt;K2:b&gt;</td>
    * <td>&lt;K2:ValueJoiner(null,b)&gt;<br />&lt;K2:ValueJoiner(B,b)&gt;</td>
    * </tr>
    * <tr>
    * <td></td>
    * <td>&lt;K3:c&gt;</td>
    * <td>&lt;K3:ValueJoiner(null,c)&gt;</td>
    * </tr>
    * </table>
    * Both input streams need to be co-partitioned on the join key.
    * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
    * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
    * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
    * user-specified in {@link StreamsConfig} via parameter
    * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
    * "-repartition" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    * <p>
    * Repartitioning can happen for one or both of the joining {@code KStream}s.
    * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
    * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
    * correctly on its key.
    * <p>
    * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
    * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
    * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
    * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
    * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
    * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
    *
    * @param otherStream the { @code KStream} to be joined with this stream
    * @param joiner      a { @link ValueJoiner} that computes the join result for a pair of matching records
    * @param windows     the specification of the { @link JoinWindows}
    * @tparam V1 the value type of the other stream
    * @tparam R  the value type of the result stream
    * @return a { @code KStream} that contains join-records for each key and values computed by the given
    *         { @link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
    *         both { @code KStream} and within the joining window intervals
    * @see #join(KStream, ValueJoiner, JoinWindows)
    * @see #leftJoin(KStream, ValueJoiner, JoinWindows)
    */
  def outerJoin[V1, R](otherStream: TKStream[K, V1], joiner: (V, V1) => R, windows: JoinWindows)(
    implicit keySerde: Serde[K],
    thisValueSerde: Serde[V],
    otherValueSerde: Serde[V1]): TKStream[K, R] = streamToTypesafe(source.outerJoin(otherStream.source, new ValueJoiner[V, V1, R] {
    override def apply(value1: V, value2: V1) = joiner(value1, value2)
  }, windows, keySerde, thisValueSerde, otherValueSerde))

}

object TKStream {
  private implicit def streamToTypesafe[I, J](source: KStream[I, J]): TKStream[I, J] = new TKStream[I, J](source)

  private implicit def groupedStreamToTypesafe[I, J](source: KGroupedStream[I, J]): TKGroupedStream[I, J] = new TKGroupedStream(source)

}