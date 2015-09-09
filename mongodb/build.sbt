import me.tfeng.toolbox._

name := "mongodb"

Settings.common

libraryDependencies ++= Seq(
  "org.mongodb" % "mongo-java-driver" % Versions.mongoDb,
  "com.novocode" % "junit-interface" % Versions.junitInterface % "test",
  "junit" % "junit" % Versions.junit % "test",
  "org.hamcrest" % "hamcrest-all" % Versions.hamcrest % "test"
)

SbtAvro.settings

me.tfeng.sbt.plugins.SbtAvro.Keys.schemataDirectories in Test := Seq("src/test/resources/schemata")
