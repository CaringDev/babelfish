package ch.fhnw.imvs.babelfish.importer

import ch.fhnw.imvs.babelfish.infrastructure.DatabaseManager.tx
import ch.fhnw.imvs.babelfish.infrastructure._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.Identifier
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.schema._
import ch.fhnw.imvs.babelfish.util.{ Logging, FileSystemWatcher }
import java.nio.file.{ Files, Paths, Path }
import java.util.zip.{ ZipEntry, ZipFile }
import org.apache.commons.csv.CSVUtils
import org.apache.commons.io.IOUtils
import org.joda.time.DateTime
import scala.util.{ Failure, Success, Try }

/** Handles importing data into Babelfish.
  *
  * Data is imported from ZIP files containing CSV text-files stored in a dedicated {IMPORT-DIR} folder.
  * The format of these CSV files is determined by [[ch.fhnw.imvs.babelfish.importer.CsvSchema]].
  * Files in the ZIP file must be named "nodeName.csv" respectively "edgeName-.csv".
  * Imported files are moved to a subdirectory for successful respectively failed imports.
  * Imports try to diff against the file matching the last import, if existent.
  * The ZIP files also must contain an [[ch.fhnw.imvs.babelfish.importer.ImporterConfiguration]].
  */
object CsvImporter extends Logging {

  /** Loads data into the database.
    *
    * If lastZipFile is specified the importer does it's best to only write the differences into the DB.
    * This works if for any given node or edge both ZIP are configured for full import.
    *
    * @param db the database to import the data into
    * @param currentZipFile the ZIP file containing the data to import
    * @param lastZipFile the ZIP file containing the data that was used to create the previous DB version, if any
    */
  def loadIntoDb(db: ReadWriteDb, currentZipFile: ZipFile, lastZipFile: Option[ZipFile]) {

    val currentConfiguration = ImporterConfiguration(currentZipFile)
    val lastConfiguration = lastZipFile.map(ImporterConfiguration(_))

    val (currentNodes, currentEdges) = zipEntriesBySchemaElement(currentZipFile, db.schema)
    val lastSources: Map[SchemaElement, ZipEntry] = lastZipFile.map(
      zipEntriesBySchemaElement(_, db.schema)).map { case (ns, es) => (ns ++ es).toMap }.getOrElse(Map())

    val missingEdges = db.schema.edges.filterNot(_.isId).toSet -- currentEdges.map(_._1)
    if (missingEdges.nonEmpty) logger.warn(s"Missing files for edges ${missingEdges.map(_.name).mkString(", ")}")
    val missingNodes = db.schema.nodes.toSet -- currentNodes.map(_._1)
    if (missingNodes.nonEmpty) logger.warn(s"Missing files for nodes ${missingNodes.map(_.name).mkString(", ")}")

    def load[T <: SchemaElement, V, I](schemaElement: T, zipEntry: ZipEntry)(
      reader: (Seq[String], Seq[(Array[String], Int)], T, Seq[NamedSchemaProperties]) => Seq[V],
      unifier: V => I,
      writer: V => Unit,
      identifier: I => Option[Identifier],
      currentElements: => Iterable[Identifier]): String = {

      val canDiff = lastConfiguration.exists(!isIncrementalImport(_, schemaElement) & lastSources.contains(schemaElement))
      val csvSchema = CsvSchema.explainSchema(db.schema).mapValues(_._2)

      logger.trace(s"Parsing $schemaElement")
      val (currentHeaders, currentValues) = loadSource(currentZipFile, zipEntry)
      val currentData = reader(currentHeaders, currentValues.zipWithIndex, schemaElement, csvSchema(schemaElement))
      logger.trace(s"Parsed ${currentData.size} $schemaElement")
      if (currentData.isEmpty) logger.debug(s"Found empty file for $schemaElement")

      if (isIncrementalImport(currentConfiguration, schemaElement)) {
        val msg = loadLog(schemaElement, 0, currentData.size, "incremental")
        currentData.foreach(writer)
        msg
      } else if (canDiff) {
        val (lastHeaders, lastValues) = loadSource(lastZipFile.get, lastSources(schemaElement))
        val lastData = reader(lastHeaders, lastValues.zipWithIndex, schemaElement, csvSchema(schemaElement))
        val currentDataSet = currentData.toSet

        val addedOrUpdated = currentDataSet -- lastData
        val deleted = lastData.map(unifier).toSet -- currentData.map(unifier)
        val msg = loadLog(schemaElement, deleted.size, addedOrUpdated.size, "differential")
        deleted.foreach { i =>
          identifier(i) match {
            case None     => logger.warn(s"Could not find edge for $i (from last import)")
            case Some(ei) => db.deleteById(ei)
          }
        }
        addedOrUpdated.foreach(writer)
        msg
      } else {
        val deleteCandidates = currentElements.toSet
        val toDelete = if (deleteCandidates.isEmpty) { // e.g. first import
          Seq()
        } else {
          deleteCandidates -- currentData.flatMap(v => identifier(unifier(v)))
        }
        val msg = loadLog(schemaElement, toDelete.size, currentData.size, "full")
        toDelete.foreach(i => db.deleteById(i))
        currentData.foreach(writer)
        msg
      }
    }

    val nodeMessages = currentNodes.map {
      case (sn, ze) =>
        tx(db) {
          load(sn, ze)(
            readNodes,
            (n: NodeData) => n.idValues,
            (n: NodeData) => db.createOrUpdateNode(sn, n),
            (id: Seq[Any]) => db.findNodeByFullId(sn, id),
            db.nodes(sn).filter(n => db.validAtHeadVersion(n)))
        }
    }.mkString("\n")

    val edgeMessages = currentEdges.map {
      case (se, ze) =>
        tx(db) {
          load(se, ze)(
            readEdges,
            (e: EdgeData) => (e.fromIds, e.toIds),
            (e: EdgeData) => db.createOrUpdateEdge(se, e),
            (ids: (Seq[Any], Seq[Any])) => db.findEdgeByData(se, ids._1, ids._2),
            db.edges(se).filter(e => db.validAtHeadVersion(e)))
        }
    }.mkString("\n")

    logger.info(s"Import summary:\n$nodeMessages\n$edgeMessages")
  }

  private def loadLog(schemaElement: SchemaElement, delSize: Int, addChangeSize: Int, importType: String): String = {
    val msg = s"$schemaElement with $importType import. ($delSize deletions, $addChangeSize additions/changes))"
    logger.debug(s"Loading $msg")
    msg
  }

  private def loadSource(zipFile: ZipFile, zipEntry: ZipEntry): (Seq[String], Seq[Array[String]]) = {
    val zipStream = zipFile.getInputStream(zipEntry)
    val source = IOUtils.toString(zipStream, "UTF-8")
    zipStream.close()
    val res = parseSource(source)
    res match {
      case None =>
        logger.warn(s"Found completely empty file ${zipEntry.getName}. Expected at least the header.")
        (Seq(), Seq())
      case Some(x) => x
    }
  }

  /** Parses a CSV source into its header line and values.
    *
    * @param source the raw CSV data
    * @return the CSV headers and values, if any.
    */
  def parseSource(source: String): Option[(Seq[String], Seq[Array[String]])] = {
    val parsedSources = CSVUtils.parse(source)
    if (parsedSources.isEmpty) {
      None
    } else {
      val headers = parsedSources.head
      val values = parsedSources.drop(1)
      Some((headers, values))
    }
  }

  /** Parses node data from strings.
    *
    * @param header the header fields
    * @param records the value fields and their originating line number (used for error messages)
    * @param schemaNode the [[ch.fhnw.imvs.babelfish.schema.SchemaNode]] the data is for
    * @param schema the ordered, named schema properties as specified by [[ch.fhnw.imvs.babelfish.importer.CsvSchema]]
    * @return the [[ch.fhnw.imvs.babelfish.importer.NodeData]] for the records.
    */
  def readNodes(header: Seq[String], records: Seq[(Array[String], Int)], schemaNode: SchemaNode, schema: Seq[NamedSchemaProperties]): Seq[NodeData] = {
    val propertyNames = schema.flatten.map(_._1)
    validate(schemaNode.toString(), header, propertyNames)
    val Seq(ids, props) = schema.map(_.map(_._2))
    for {
      (record, nr) <- records
      idProps <- parseRecord(record, ids, nr)
      dataProps <- parseRecord(record.drop(idProps.size), props, nr)
    } yield NodeData(idProps, dataProps)
  }

  /** Parses edge data from strings.
    *
    * @param header the header fields
    * @param records the value fields and their originating line number (used for error messages)
    * @param edge the [[ch.fhnw.imvs.babelfish.schema.SchemaEdge]] the data is for
    * @param schema the ordered, named schema properties as specified by [[ch.fhnw.imvs.babelfish.importer.CsvSchema]]
    * @return the [[ch.fhnw.imvs.babelfish.importer.EdgeData]] for the records.
    */
  def readEdges(header: Seq[String], records: Seq[(Array[String], Int)], edge: SchemaEdge, schema: Seq[Seq[(String, SchemaElement#SchemaProperty[_])]]): Seq[EdgeData] = {
    val propertyNames = schema.flatten.map(_._1)
    validate(edge.toString, header, propertyNames)
    val Seq(from, to, props) = schema.map(_.map(_._2))
    for {
      (record, nr) <- records
      fromIds <- parseRecord(record, from, nr)
      toIds <- parseRecord(record.drop(fromIds.size), to, nr)
      propVals <- parseRecord(record.drop(fromIds.size + toIds.size), props, nr)
    } yield EdgeData(fromIds, toIds, propVals)
  }

  private def validate(name: String, header: Seq[String], propertyNames: Seq[String]) {
    if (header.zip(propertyNames).exists(hp => hp._1 != hp._2) || header.size != propertyNames.size) {
      val props = propertyNames.mkString(", ")
      val heads = header.mkString(", ")
      throw new IllegalArgumentException(s"Fields (in order) for '$name' should be '$props' but were '$heads'")
    }
  }

  private def parseRecord(values: Seq[String], properties: Seq[SchemaElement#SchemaProperty[_]], recordNo: Int): Option[Seq[Any]] = {
    val valuesAndProperties = values.zip(properties)
    val fields: Seq[Any] = valuesAndProperties.flatMap(vp => parseField(vp._1, vp._2, recordNo))
    if (fields.size == valuesAndProperties.size) Some(fields)
    else None
  }

  private def parseField(value: String, property: SchemaElement#SchemaProperty[_], recordNo: Int): Option[Any] = {
    // do not use .getOrElse here (avoid creating an anonymous function for every field)
    property.neoType.parseFromString(value) match {
      case None =>
        logger.warn(s"Value '$value' in record $recordNo is not of type '${property.neoType.name}' for '${property.parent.name}.${property.name}'")
        None
      case x: Some[_] => x
    }
  }

  private def zipEntriesBySchemaElement(zipFile: ZipFile, schema: Schema): (Seq[(SchemaNode, ZipEntry)], Seq[(SchemaEdge, ZipEntry)]) = {
    import scala.collection.JavaConverters._
    // Use '/' for path filtering in ZipFiles (ZipEntry does not use OS path separator)
    val csvRootEntries = zipFile.entries().asScala.filter(_.getName.endsWith(".csv")).filterNot(_.getName.contains('/'))
    val elements = (schema.nodes ++ schema.edges).map(e => e.name -> e).toMap
    val (nodes, edges) = csvRootEntries.foldLeft((List[(SchemaNode, ZipEntry)](), List[(SchemaEdge, ZipEntry)]())) {
      case ((ns, es), ze) =>
        val rawName = ze.getName.dropRight(4)
        elements.get(rawName) match {
          case None =>
            logger.warn(s"Could not find a schema element $rawName")
            (ns, es)
          case Some(sn: SchemaNode) => (sn -> ze :: ns, es)
          case Some(se: SchemaEdge) if se.isId =>
            logger.warn(s"Will not import auto-created ID-edge ${se.name}")
            (ns, es)
          case Some(se: SchemaEdge) if !se.isId =>
            (ns, se -> ze :: es)
        }
    }

    val nodeOrder = schema.nodes.zipWithIndex.toMap
    val sortedNodes = nodes.sortBy { case (schemaNode, _) => nodeOrder(schemaNode) }
    (sortedNodes, edges)
  }

  private def isIncrementalImport(config: ImporterConfiguration, s: SchemaElement) = {

    config.byFile.get(s.name).map(_.get(ImporterConfiguration.IMPORT) match {
      case Some(ImporterConfiguration.Import.INCREMENTAL) => true
      case Some(ImporterConfiguration.Import.FULL)        => false
      case _                                              => throw new IllegalArgumentException(s"Configuration value for ${s.name}.import must be '${ImporterConfiguration.Import.INCREMENTAL}' or '${ImporterConfiguration.Import.FULL}' in ${config.zipFile.getName}")
    }).getOrElse {
      logger.warn(s"Using FULL import configuration for ${s.name} in ${config.zipFile.getName}. Please specify an explicit configuration.")
      false
    }
  }

  val importedDir = "imported"

  /** Creates a [[ch.fhnw.imvs.babelfish.importer.CsvImporter]] for a given Babelfish instance and import folder.
    *
    * Once created a [[ch.fhnw.imvs.babelfish.importer.CsvImporter]] immediately starts monitoring the sourceDir until `kill`ed.
    * ZIP files already stored in the import folder will not be imported.
    * The importer needs sufficient access rights to create / move files within the import folder and its
    * subdirectories `imported` and `failed`. These subdirectories will be created on demand if they don't exist.
    *
    * @param bf the Babelfish instance to import the data into
    * @param sourceDir the directory in which to watch for ZIP files
    * @return a [[ch.fhnw.imvs.babelfish.infrastructure.Killable]] to use for stopping the importer.
    */
  def apply(bf: Babelfish, sourceDir: Path): Killable = new Killable {
    private val importTriggers = new FileSystemWatcher(sourceDir, ".zip", f => new ZipFile(f.toFile))(importZip)

    private def importZip(currentZipFile: ZipFile) {
      val currentZipFileName = currentZipFile.getName

      logger.info(s"Starting import of $currentZipFileName")
      val importedPath = sourceDir.resolve(importedDir)
      Files.createDirectories(importedPath)

      val importFunction = (db: ReadWriteDb) => {
        val lastZipFile = locateLastZipFile(importedPath, db.version.previous)
        val tried = Try { loadIntoDb(db, currentZipFile, lastZipFile) }
        lastZipFile.foreach(_.close())
        if (tried.isFailure) {
          logger.warn(s"Failed import (${tried.failed.get.getMessage})", tried.failed.get)
          throw tried.failed.get
        }
        db.version
      }

      import scala.concurrent.ExecutionContext.Implicits.global
      bf.runWriteJob(importFunction).onComplete { res =>
        val zipFile = Paths.get(currentZipFile.getName)
        currentZipFile.close()
        res match {
          case Success(newVersion) =>
            Files.move(zipFile, importedPath.resolve(s"$newVersion.zip"))
            logger.info(s"Import of $currentZipFileName finished")
          case Failure(ex) =>
            val targetPath = sourceDir.resolve("failed")
            Files.createDirectories(targetPath)
            Files.move(zipFile, targetPath.resolve(s"${DateTime.now().toString("yyyyMMdd-HHmmss.S")}-${zipFile.getFileName}"))
            logger.warn("Error in import job", ex)
        }
      }
    }

    /** Stops the importer, i.e. watching for ZIP files. */
    def kill() { importTriggers.kill() }

    logger.info(s"Waiting for import files in $sourceDir")
  }

  private def locateLastZipFile(importedDir: Path, version: Version): Option[ZipFile] = {
    val lastZipFile = importedDir.resolve(s"$version.zip").toFile
    if (lastZipFile.exists()) Some(new ZipFile(lastZipFile)) else None
  }
}