import com.earldouglas.xsbtwebplugin.WebPlugin
import sbt.Keys._
import sbt._

trait Web { this: BuildSettings with Core with DSL =>
  final val JETTY_VERSION = "9.1.0.v20131115"
  final val ScalatraVersion = "2.3.0.RC3"

  lazy val web = Project("Web", file("Web"), settings = commonSettings ++ WebPlugin.webSettings ++ Seq(
    name := "Babelfish Web",
    version := PRODUCT_VERSION,
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => artifact.name + "." + artifact.extension },
    libraryDependencies ++= Seq(
      "org.scalatra" %% "scalatra" % ScalatraVersion,
      "org.scalatra" %% "scalatra-json" % ScalatraVersion,
      "org.json4s" %% "json4s-jackson" % "3.2.10",
      "org.eclipse.jetty" % "jetty-webapp" % JETTY_VERSION % "container",
      "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container;provided;test" artifacts Artifact("javax.servlet", "jar", "jar"),
      "org.eclipse.jetty" % "jetty-jsp" % JETTY_VERSION % "container",
      "com.google.guava" % "guava" % "17.0",
      "com.google.code.findbugs" % "jsr305" % "2.0.3"
    )
  )) dependsOn (core % "test->test;compile->compile", dsl)
}