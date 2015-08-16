import me.tfeng.toolbox._

name := "toolbox"

Settings.common ++ Settings.disablePublishing

lazy val parent =
    project in file(".") aggregate(titan)

lazy val titan = project in file("titan")
