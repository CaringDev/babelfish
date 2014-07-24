package ch.fhnw.imvs.babelfish

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.MigrationAPI
import ch.fhnw.imvs.babelfish.schema.{ SchemaElement, Schema }
import ch.fhnw.imvs.babelfish.util.Logging
import java.nio.file.Path

trait ConfigurationBase extends Logging {
  /** The (singleton) instance of the database schema*/
  def schema: Schema
  /** FQN of the DSL cake, e.g. ch.fhnw.imvs.CarolsDSL*/
  def dslBase: String
  /** Imports to be added to the DSL interpreter */
  def dslImports: Seq[String]
  /** Lookup  for schema migrations */
  def default(p: SchemaElement#SchemaProperty[_]): Any = { logger.error(s"Asked for unknown default of $p"); ??? }
  /** Low-level migration, if any */
  def lowlevelMigration: Option[MigrationAPI => Unit] = None
}

private[babelfish] trait InternalConfiguration extends ConfigurationBase {
  private[babelfish] def configurationFile: Path
  private[babelfish] def configBFversion: String
  private[babelfish] def version: String
}

class PlainSchemaConfiguration(s: Schema, default: => Any = ???) extends InternalConfiguration {
  private[babelfish] def configurationFile: Path = ???
  val schema: Schema = s
  val dslBase: String = ""
  val dslImports: Seq[String] = Seq()
  override def default(p: SchemaElement#SchemaProperty[_]): Any = default

  private[babelfish] def configBFversion: String = "None"
  private[babelfish] def version: String = "None"
}