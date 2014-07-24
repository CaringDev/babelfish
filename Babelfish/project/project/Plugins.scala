import sbt._

object Plugins extends Build {
  override def projects = Seq(root)
  lazy val root = Project("plugins", file("."), settings = Defaults.defaultSettings) dependsOn junitXmlListener
  lazy val junitXmlListener = uri("http://github.com/rasch/junit_xml_listener.git#7658ee513e9767f6056adf7d245ec2948b84c33c")
}