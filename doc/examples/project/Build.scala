import sbt._
import sbt.Keys._
import scala._
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

object Examples extends Build {
  val babelfishVersion = "1.3"
  val configurationVersion = "1.0"

  lazy val root = Project("Examples", file("."), settings = scalariformSettings ++ Seq(
    scalaVersion := "2.11.1",
    organization := "ch.fhnw.imvs",
    version := configurationVersion,
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "BabelfishConfiguration." + artifact.extension },
    packageOptions += Package.ManifestAttributes(
      "BabelfishVersion" -> babelfishVersion,
      "ConfigurationVersion" -> configurationVersion
    ),
    libraryDependencies ++= Seq(
      "ch.fhnw.imvs" %% "babelfish-core" % babelfishVersion,
      "ch.fhnw.imvs" %% "babelfish-dsl" % babelfishVersion,
      "org.scalatest" %% "scalatest" % "2.1.7" % "test"
    ),
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 50)
      .setPreference(CompactStringConcatenation, false)
      .setPreference(DoubleIndentClassDeclaration, false)
      .setPreference(FormatXml, true)
      .setPreference(IndentLocalDefs, false)
      .setPreference(IndentPackageBlocks, true)
      .setPreference(IndentSpaces, 2)
      .setPreference(PreserveDanglingCloseParenthesis, false)
      .setPreference(PreserveSpaceBeforeArguments, true) // for scalatest "... equal (2)" instead of "... equal(2)"
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(SpaceBeforeColon, false)
      .setPreference(SpaceInsideBrackets, false)
      .setPreference(SpaceInsideParentheses, false)
      .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
      .setPreference(MultilineScaladocCommentsStartOnFirstLine, true),
    testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),
    concurrentRestrictions := Seq(Tags.exclusive(Tags.All))
  ))
}