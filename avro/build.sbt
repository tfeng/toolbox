import me.tfeng.toolbox._

name := "avro"

Settings.common

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson,
  "org.apache.avro" % "avro" % Versions.avro
)
