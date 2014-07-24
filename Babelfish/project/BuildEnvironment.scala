import sbt.Keys._
import sbt._

trait BuildEnvironment {
  val webInf = "Web/src/main/webapp/WEB-INF/"
  val coreResources = "Core/src/main/resources/"

  val envDependentFiles = Seq(
    (webInf, "web", ".xml"),
    (coreResources, "logback", ".xml")
  )

  def envFile(path: String, name: String, ext: String, env: String) {
    IO.transfer(new File(path+name+env+ext), new File(path+name+ext))
  }

  val setDevTask = TaskKey[Unit]("set-dev", "Sets build parameters for development.") <<= streams map { (s: TaskStreams) =>
    s.log.info("DEVELOPMENT environment")
    envDependentFiles.foreach(f => envFile(f._1, f._2, f._3, ".dev"))
  }

  val setProdTask = TaskKey[Unit]("set-prod", "Sets build parameters for production.") <<= streams map { (s: TaskStreams) =>
    s.log.info("PRODUCTION environment")
    envDependentFiles.foreach(f => envFile(f._1, f._2, f._3, ".prod"))
  }
}