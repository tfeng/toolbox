import me.tfeng.toolbox._

name := "dust"

Settings.common

libraryDependencies ++= Seq(
  "org.webjars" % "dustjs-linkedin" % Versions.dustjs,
  "org.webjars" % "webjars-locator" % Versions.webjarsLocator
)
