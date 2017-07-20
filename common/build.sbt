import me.tfeng.toolbox._

name := "common"

Settings.common

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % Versions.logback,
  "com.google.guava" % "guava" % Versions.guava,
  "org.apache.commons" % "commons-text" % Versions.commonsText,
  "org.slf4j" % "slf4j-api" % Versions.slf4j
)
