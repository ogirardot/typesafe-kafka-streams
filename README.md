# typesafe-kafka-streams [![Build Status](https://travis-ci.org/ogirardot/typesafe-kafka-streams.svg?branch=master)](https://travis-ci.org/ogirardot/typesafe-kafka-streams)
A started draft of a typesafe - scala - compatible API

## How to use
The artifact is deployed on sonatype's central repository, so all you need is to add it to your resolvers and use it like that : 

```
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "fr.psug.kafka" %% "typesafe-kafka-streams" % "X.X.X-SNAPSHOT"
```

Or to use the latest stable release

```
resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "fr.psug.kafka" %% "typesafe-kafka-streams" % "0.2.1"
```

## In practice
You've got two options :

* You can improve your current `KStream[K, V]` 
* Or use a dedicated trait `TKafkaStreams` to create a typesafe KStream directly for you.

### Improving your existing KStream
Once you've created your `KStream[K, V]` you can use the `.typesafe` method to get a Scala friendly Kakfa Stream API : 
```
import fr.psug.kafka.streams.KafkaStreamsImplicits._

val streams: KStream[String, String] = ???
streams.typesafe
  .filter((k, v) => k.startsWith("valid_"))
  .to("validated_topic") // this part expects implicit Serdes in scope so we need to import fr.psug.kafka.streams.KafkaStreamsImplicitSerdes._
```

### Creating a typesafe KStream directly
You just need to use the `TKafkaStreams` trait, it will help you bootstrap and start your application. To use it :

* Define all the variables/methods needed by the API
* Then use the `source(topic: String)` method to create as many new TKStream as you need providing the implicits needed
* And finally call `start(props)` when you're done.

## What is different in this API

A few key points make this draft easier to use than the original Java 8 API :

* It is Scala friendly in terms of types. for example `Predicates[K, V]` are `(K, V) => Boolean`.
* `Serde`(s) are no longer explicit parameters, but rather `implicit` ones. So if you've got the proper implicits in scope, you won't need to explicitely pass the Serdes as parameters.
* we also added a few perks with for example the `partition` method that does not exists in the KStream API : 
```
val (validated, rejected) = data.partition((_, v) => v.isRight)
```
