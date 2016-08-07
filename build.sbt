import me.tfeng.toolbox._

name := "toolbox"

Settings.common ++ Settings.disablePublishing

lazy val parent = project in file(".") aggregate(avro, common, dust, kafka, mongodb, spring)

lazy val common = project

lazy val avro = project dependsOn(common)

lazy val spring = project dependsOn(common)

lazy val kafka = project dependsOn(avro)

lazy val dust = project dependsOn(spring)

lazy val mongodb = project dependsOn(avro, spring)
