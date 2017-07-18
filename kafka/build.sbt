import me.tfeng.toolbox._

name := "kafka"

Settings.common

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.12" % Versions.kafka
)
