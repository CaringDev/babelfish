import sbt.Keys._
import sbt._

trait DSL { this: BuildSettings with Core =>
  lazy val dsl = Project("DSL", file("DSL"), settings = commonSettings ++ Seq(
    name := "Babelfish DSL",
    version := PRODUCT_VERSION,
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % SCALA_VERSION,
      "org.hsqldb" % "hsqldb" % "2.3.2"
    )
  )) dependsOn (core % "test->test;compile->compile")
}