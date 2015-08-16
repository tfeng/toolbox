import me.tfeng.toolbox._

name := "spring"

Settings.common

libraryDependencies ++= Seq(
  "org.springframework" % "spring-aop" % Versions.spring,
  "org.springframework" % "spring-context" % Versions.spring,
  "org.springframework" % "spring-expression" % Versions.spring
)
