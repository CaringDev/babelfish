import sbt.Keys._
import sbt._

trait Core { this: BuildSettings =>
  val NEO_VERSION = "2.0.3"
  lazy val core = Project("Core", file("Core"), settings = commonSettings ++ Seq(
    name := "Babelfish Core",
    version := PRODUCT_VERSION,
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % "2.3",
      "org.joda" % "joda-convert" % "1.6",
      "org.neo4j" % "neo4j-kernel" % NEO_VERSION,
      "org.neo4j" % "neo4j-lucene-index" % NEO_VERSION,
      "org.neo4j" % "neo4j-kernel" % NEO_VERSION % "test" classifier "tests"
    ),
    sourceGenerators in Compile <+= (sourceManaged in Compile, version, name) map { (d, v, n) =>
      val file = d / "ch" / "fhnw" / "imvs" / "babelfish" / "util" / "Info.scala"
      IO.write(file, """package ch.fhnw.imvs.babelfish.util
                       |object Info {
                       |  val version = "%s"
                       |  val name = "%s"
                       |}
                       |""".stripMargin.format(v, n))
      Seq(file)
    }
  ))
}