import me.tfeng.toolbox._

name := "mongodb"

Settings.common

libraryDependencies ++= Seq(
  "org.mongodb" % "mongo-java-driver" % Versions.mongoDb,
  "com.novocode" % "junit-interface" % Versions.junitInterface % "test",
  "junit" % "junit" % Versions.junit % "test",
  "org.hamcrest" % "hamcrest-all" % Versions.hamcrest % "test"
)

Avro.settings

Avro.Keys.schemataDirectories in Test := Seq(baseDirectory.value / "src" / "test" / "resources" / "schemata")
