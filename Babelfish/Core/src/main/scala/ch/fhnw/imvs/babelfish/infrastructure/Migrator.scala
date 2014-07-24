package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.ConfigurationBase
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.MigrationAPI
import ch.fhnw.imvs.babelfish.schema._
import ch.fhnw.imvs.babelfish.util.{ Logging, FileSystemWatcher }
import java.nio.file.{ NoSuchFileException, Files, Path }
import scala.util._

object Migrator extends Logging {

  private val migrationJarName = "Migration.jar"

  def apply(bf: Babelfish, importPath: Path, dbPath: Path): Killable = new Killable {
    private val trigger = new FileSystemWatcher(importPath, ".jar", identity)(migrate)

    private def migrate(file: Path) {
      import scala.concurrent.ExecutionContext.Implicits.global
      bf.runMigrationJob { mAPI =>
        val newFile = dbPath.resolve(DatabaseManager.WRITE_DIR).resolve(mAPI.queryDb.version.v.toString).resolve(Configuration.configJar)
        logger.debug(s"Moving $file to $newFile")

        // Delete the file which could reside at the location where we want to move the file to
        try {
          Files.delete(newFile)
        } catch {
          case ex: NoSuchFileException => logger.debug(s"Nothing to delete: File does not exists: $newFile")
        }

        Files.move(file, newFile)
        // use a temporary file s.t. we can delete the WRITE_DIR
        val tempDir = Files.createTempDirectory("bfMigration")
        val tempFile = tempDir.resolve(Configuration.configJar).toAbsolutePath
        logger.trace(s"Copying $newFile to $tempFile")
        Files.copy(newFile, tempFile)

        val newConfig = new Configuration(tempFile, mAPI.queryDb.version)

        if (file.getFileName.toString == Configuration.configJar)
          DatabaseManager.tx(mAPI)(Migrator.autoMigrate(mAPI, newConfig))
        else if (file.getFileName.toString == migrationJarName)
          Migrator.lowlevelMigrate(mAPI, newConfig)
        else
          throw new IllegalArgumentException(
            s"Migration JARs must either be named '${Configuration.configJar}' for automatic diff migration or $migrationJarName for lowlevel migration.")

        tempFile.toFile.deleteOnExit()
      }.onComplete {
        case Success(_) => logger.info("Migration finished")
        case Failure(e) => logger.error("Failure during migration", e)
      }
    }

    def kill() { trigger.kill() }

    logger.info(s"Waiting for migration jar in ${importPath.toAbsolutePath.toString}")
  }

  def autoMigrate(mAPI: MigrationAPI, newConfig: ConfigurationBase) {
    def propertiesByParent(s: SchemaEqWrapper) = (s.nodes.map(n => n.name -> n.properties) ++ s.edges.map(e => e.name -> e.properties)).toMap

    val oldSchema = SchemaEqWrapper(mAPI.queryDb.schema)
    val newSchema = SchemaEqWrapper(newConfig.schema)

    val oldNodesByName = oldSchema.nodes.map(n => n.name -> n).toMap
    val newNodesByName = newSchema.nodes.map(n => n.name -> n).toMap
    val oldEdgesByName = oldSchema.edges.map(n => n.name -> n).toMap
    val newEdgesByName = newSchema.edges.map(n => n.name -> n).toMap
    val oldPropertiesByParent = propertiesByParent(oldSchema)
    val newPropertiesByParent = propertiesByParent(newSchema)
    val (removedIdProps, removedProps) = oldPropertiesByParent.flatMap { case (n, ps) => ps -- newPropertiesByParent.getOrElse(n, Seq()) }.partition(_.isId)
    val (addedIdProps, addedProps) = newPropertiesByParent.flatMap { case (n, ps) => ps -- oldPropertiesByParent.getOrElse(n, Seq()) }.partition(_.isId)
    val idPropsForExistingNodes = addedIdProps.filter { p => oldSchema.nodes.map(_.name).contains(p.parentName) }

    // ID elements
    if (removedIdProps.nonEmpty)
      throw new IllegalArgumentException(s"Removing id properties ${removedIdProps.map(_.property)} is not supported.")
    val removedIdEdges = oldSchema.edges.filter(_.isId) -- newSchema.edges.filter(_.isId)
    if (removedIdEdges.nonEmpty)
      throw new IllegalArgumentException(s"Removing id edges ${removedIdEdges.map(_.edge)} is not supported.")
    // nodes
    val removedNodes = (oldNodesByName.keys.toSet -- newNodesByName.keys).map(oldNodesByName(_))
    if (removedNodes.nonEmpty) throw new NotImplementedError(s"Removing nodes ${removedNodes.map(_.node)} is not supported (yet).")
    val addedNodes = (newNodesByName.keys.toSet -- oldNodesByName.keys).map(newNodesByName(_))
    addedNodes.foreach(n => mAPI.addNode(n.node))
    // edges
    val removedEdges = (oldEdgesByName.keys.toSet -- newEdgesByName.keys).map(oldEdgesByName(_))
    if (removedEdges.nonEmpty) throw new NotImplementedError(s"Removing edges ${removedEdges.map(_.edge)} is not supported (yet).")
    val (addedIdEdges, addedEdges) = (newEdgesByName.keys.toSet -- oldEdgesByName.keys).map(newEdgesByName(_)).partition(_.isId)
    val idEdgesForExistingNodes = addedIdEdges.filter(e => oldNodesByName.contains(e.from))
    if (idEdgesForExistingNodes.nonEmpty)
      throw new IllegalArgumentException(s"Adding id edges ${idEdgesForExistingNodes.map(_.edge)} to existing nodes is not supported.")
    def atLeastOne(c: Cardinality) = c == Cardinality.+ || c == Cardinality.One
    val atLeastOneEdgesForExistingNodes = addedEdges.filter { e =>
      oldNodesByName.contains(e.from) && atLeastOne(e.edge.toCard) ||
        oldNodesByName.contains(e.to) && atLeastOne(e.edge.fromCard)
    }
    if (atLeastOneEdgesForExistingNodes.nonEmpty)
      throw new IllegalArgumentException(s"Adding edges ${atLeastOneEdgesForExistingNodes.map(_.edge)} that have cardinality '1' or '+' is not supported.")
    (addedIdEdges ++ addedEdges).foreach(e => mAPI.addEdge(e.edge))

    // properties
    removedProps.foreach(p => mAPI.migrationDb.removeProperties(p.property))
    idPropsForExistingNodes.foreach(p => mAPI.addIdProperty(p.property, newConfig.default))
    addedProps.foreach(p => mAPI.addProperty(p.property, newConfig.default))
  }

  def lowlevelMigrate(mAPI: MigrationAPI, migrationJar: Configuration) {
    migrationJar.lowlevelMigration match {
      case None            => throw new IllegalArgumentException("Found no lowlevel migration definition.")
      case Some(migration) => migration(mAPI)
    }
  }

  private case class PropertyEqWrapper(name: String, isId: Boolean, tpeName: String, parentName: String)(val property: SchemaElement#SchemaProperty[_])
  private case class EdgeEqWrapper(name: String, from: String, to: String, isId: Boolean, properties: Set[PropertyEqWrapper])(val edge: SchemaEdge)
  private case class NodeEqWrapper(name: String, properties: Set[PropertyEqWrapper])(val node: SchemaNode)
  private case class SchemaEqWrapper(nodes: Set[NodeEqWrapper], edges: Set[EdgeEqWrapper])

  private object PropertyEqWrapper {
    def apply[T](p: SchemaElement#SchemaProperty[T]) = new PropertyEqWrapper(p.name, p.isId, p.neoType.stringRepresentation, p.parent.name)(p)
  }
  private object EdgeEqWrapper {
    def apply(e: SchemaEdge) = new EdgeEqWrapper(e.name, e.from.name, e.to.name, e.isId, e.properties.map(PropertyEqWrapper(_)).toSet)(e)
  }
  private object NodeEqWrapper {
    def apply(n: SchemaNode) = new NodeEqWrapper(n.name, n.properties.map(PropertyEqWrapper(_)).toSet)(n)
  }
  private object SchemaEqWrapper {
    def apply(s: Schema) = new SchemaEqWrapper(s.nodes.map(NodeEqWrapper(_)).toSet, s.edges.map(EdgeEqWrapper(_)).toSet)
  }
}
