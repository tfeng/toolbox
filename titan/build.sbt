import me.tfeng.toolbox._

name := "titan"

Settings.common

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % Versions.logback,
  "org.mongodb" % "mongo-java-driver" % Versions.mongoDb,
  "org.slf4j" % "slf4j-api" % Versions.slf4j,
  "org.springframework" % "spring-context" % Versions.spring,
  "com.thinkaurelius.titan" % "titan-core" % Versions.titan,
  "org.apache.tinkerpop" % "gremlin-core" % Versions.gremlin
)
