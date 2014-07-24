package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.infrastructure.Configuration._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.MigrationAPI
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.schema.{ SchemaElement, Schema }
import ch.fhnw.imvs.babelfish.util.{ Logging, Info }
import ch.fhnw.imvs.babelfish.{ InternalConfiguration, ConfigurationBase }
import java.io.FileNotFoundException
import java.net.URLClassLoader
import java.nio.file.{ Paths, Files, Path }
import java.util.jar.JarFile
import scala.util.Try

/** Provides global access to Babelfish user configuration:
  * - Loaded from BabelfishConfiguration.jar
  * - Contains Schema and DSL Extensions
  */
class Configuration(dir: Path, v: Version) extends InternalConfiguration with Logging {

  private[babelfish] lazy val configurationFile: Path = {
    var confCandidate = dir.resolve(v.v.toString).resolve(configJar)
    if (!Files.exists(confCandidate)) {
      confCandidate = dir.getParent.resolve(configJar)
      logger.trace(s"Trying $confCandidate")
    }
    if (!Files.exists(confCandidate)) {
      confCandidate = Paths.get(configJar)
      logger.trace(s"Trying $configJar on classpath.")
    }
    if (!Files.exists(confCandidate)) throw new FileNotFoundException("Could not find configuration jar in dbDir/{version}, dbDir or classpath.")
    logger.debug(s"Using $confCandidate")
    confCandidate
  }

  private lazy val config = Try {
    new URLClassLoader(Array(configurationFile.toUri.toURL), this.getClass.getClassLoader)
      .loadClass(s"$configClass$$").getField("MODULE$").get(null).asInstanceOf[ConfigurationBase]
  }.getOrElse{
    val msg = s"Could not find object $configClass in $configJar."
    logger.error(msg)
    throw new ClassNotFoundException(msg)
  }

  lazy val (version, configBFversion) = {
    val mainAttributes = new JarFile(configurationFile.toFile).getManifest.getMainAttributes
    val bfConfV = mainAttributes.getValue("BabelfishVersion")
    val confV = mainAttributes.getValue("ConfigurationVersion")
    if (bfConfV == null || bfConfV < Info.version) {
      logger.warn(s"Configuration version $bfConfV does not match ${Info.version}. Continuing at your risk...")
    }
    (confV, bfConfV)
  }

  lazy val schema: Schema = config.schema
  lazy val dslBase = config.dslBase
  lazy val dslImports = config.dslImports
  override def default(p: SchemaElement#SchemaProperty[_]) = config.default(p)
  override def lowlevelMigration: Option[MigrationAPI => Unit] = config.lowlevelMigration
}

object Configuration {
  val configJar = "BabelfishConfiguration.jar"
  val configClass = "ch.fhnw.imvs.babelfish.BabelfishConfiguration"
}