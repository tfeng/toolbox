import me.tfeng.toolbox._

name := "titan"

Settings.common

libraryDependencies ++= Seq(
  "org.mongodb" % "mongo-java-driver" % Versions.mongoDb,
  "com.thinkaurelius.titan" % "titan-core" % Versions.titan,
  "org.apache.tinkerpop" % "gremlin-core" % Versions.gremlin
)
