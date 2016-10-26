import de.heikoseeberger.sbtheader.license.MIT

name := "typesafe-kafka-streams"

organization := "com.github.ogirardot.kafka"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

val kafkaVersion = "0.10.0.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)

resolvers in ThisBuild ++= Seq(
  "conjars.org" at "http://conjars.org/repo",
  "confluent" at "http://packages.confluent.io/maven/",
  "cakesolutions" at "http://dl.bintray.com/cakesolutions/maven/"
)

publishTo := Some("conjars.org" at "http://conjars.org/repo")

headers := Map(
  "scala" -> MIT("2016", "Fred Cecilia, Valentin Kasas, Olivier Girardot")
)