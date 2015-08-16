import me.tfeng.toolbox._

name := "toolbox"

Settings.common ++ Settings.disablePublishing

lazy val parent = project in file(".") aggregate(avro, common, mongodb, spring, titan)

lazy val common = project in file("common")

lazy val spring = project in file("spring") dependsOn(common)

lazy val avro = project in file("avro") dependsOn(spring)

lazy val mongodb = project in file("mongodb") dependsOn(avro % "test->compile", common % "test->test", spring)

lazy val titan = project in file("titan") dependsOn(spring)
