import sbt._
import sbt.Keys._
import scala._

object Babelfish extends Build with BuildSettings with BuildEnvironment with Core with Web with DSL {
  lazy val root = Project("Babelfish", file("."), settings = minimalSettings ++ Seq(
    publishArtifact := false,
    setDevTask, setProdTask
  )) aggregate(core, dsl, web)
}