import me.tfeng.toolbox._

name := "common"

Settings.common

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % Versions.logback,
  "com.google.guava" % "guava" % Versions.guava,
  "org.apache.commons" % "commons-lang3" % Versions.commonsLang3,
  "org.slf4j" % "slf4j-api" % Versions.slf4j,
  "com.novocode" % "junit-interface" % Versions.junitInterface % "test",
  "junit" % "junit" % Versions.junit % "test",
  "org.hamcrest" % "hamcrest-all" % Versions.hamcrest % "test"
)
