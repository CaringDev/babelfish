import com.typesafe.sbt.SbtScalariform._
import de.johoop.findbugs4sbt.Effort
import de.johoop.findbugs4sbt.FindBugs.{findbugsSettings, findbugsEffort}
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import de.johoop.jacoco4sbt.{HTMLReport, XMLReport}
import org.scalastyle.sbt.ScalastylePlugin
import sbt.Keys._
import sbt._
import scalariform.formatter.preferences._

trait BuildSettings {
  final val PRODUCT_VERSION = "1.3"

  final val SCALA_VERSION = "2.11.1"

  val minimalSettings =
      Seq(
        concurrentRestrictions := Seq(Tags.exclusive(Tags.All)),
        organization := "ch.fhnw.imvs",
        scalaVersion := SCALA_VERSION,
        scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-target:jvm-1.7", "-Xlint")
      ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings

  val standardTest = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oF")
  )

  val commonSettings = minimalSettings ++ scalariformSettings ++ ScalastylePlugin.Settings ++ findbugsSettings ++ jacoco.settings ++ Seq(
    resolvers ++= Seq(
      "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
      "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
      "linter" at "http://hairyfotr.github.io/linteRepo/releases"
    ),
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.2",
      "org.slf4j" % "jul-to-slf4j" % "1.7.7",
      "commons-io" % "commons-io" % "2.4",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      "org.apache.servicemix.bundles" % "org.apache.servicemix.bundles.commons-csv" % "1.0-r706900_3",
      "org.scalatest" %% "scalatest" % "2.1.7" % "test"
    ),
    addCompilerPlugin("com.foursquare.lint" %% "linter" % "0.1-SNAPSHOT"),
    testOptions in Test := standardTest,
    testOptions in jacoco.Config := standardTest,
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
    findbugsEffort := Effort.Maximum,

    testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),

    jacoco.reportFormats in jacoco.Config := Seq(XMLReport("utf-8"), HTMLReport("utf-8"))
  )
}