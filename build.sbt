import de.heikoseeberger.sbtheader.license.MIT

name := "typesafe-kafka-streams"

organization := "fr.psug.kafka"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.11", "2.12.3")

val kafkaVersion = "0.10.2.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)

resolvers in ThisBuild ++= Seq(
  "conjars.org" at "http://conjars.org/repo",
  "confluent" at "http://packages.confluent.io/maven/",
  "cakesolutions" at "http://dl.bintray.com/cakesolutions/maven/",
  Resolver.sonatypeRepo("snapshot"),
  Resolver.sonatypeRepo("releases")
)

headers := Map(
  "scala" -> MIT("2016", "Fred Cecilia, Valentin Kasas, Olivier Girardot")
)

// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "fr.psug"

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>https://github.com/ogirardot/typesafe-kafka-streams</url>
    <licenses>
      <license>
        <name>MIT</name>
        <url>https://opensource.org/licenses/MIT</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/ogirardot/typesafe-kafka-streams</connection>
      <developerConnection>scm:git:git@github.com:ogirardot/typesafe-kafka-streams</developerConnection>
      <url>github.com/ogirardot/typesafe-kafka-streams</url>
    </scm>
    <developers>
      <developer>
        <id>ogirardot</id>
        <name>Olivier GIRARDOT</name>
        <url>https://www.linkedin.com/in/oliviergirardot</url>
      </developer>
    </developers>
}