import me.tfeng.toolbox._

name := "mongodb"

Settings.common

libraryDependencies ++= Seq(
  "org.mongodb" % "mongo-java-driver" % Versions.mongoDb
)

SbtAvro.settings

me.tfeng.sbt.plugins.SbtAvro.Keys.schemataDirectories in Test := Seq("src/test/resources/schemata")
