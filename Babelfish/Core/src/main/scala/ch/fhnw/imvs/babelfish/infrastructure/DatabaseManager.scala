package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.util.Logging
import java.nio.file._
import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.factory.{ GraphDatabaseSettings, GraphDatabaseFactory }
import org.neo4j.graphdb.{ Transaction, GraphDatabaseService }
import scala.util.{ Failure, Success, Try }

/** Manages the databases and their storage locations. All database storage is located in one folder which is organized as follows:
  *
  * DB_Base_Dir
  * |- READ_DIR
  * |   |- 0
  * |   |  |- Neo4j Files for read-only db version 0 (Initial version after first import)
  * |   |- 1
  * |   |  |- Neo4j Files for read-only db version 1
  * :   :  :
  * |   |- n
  * |   |  |- Neo4j Files for read-only db version n
  * |- WRITE_DIR
  * |   |-(n+1)
  * |   |  |- Neo4j Files for the next read-write db
  *
  * Note that the database with version 'n' contains the data of all previous versions.
  * The WRITE_DIR is used only during a write job.
  */
object DatabaseManager extends Logging {
  /** Name of the read-only database storage location sub folder. */
  private[infrastructure] val READ_DIR = "r"
  /** Name of the read-write database storage location sub folder. */
  private[infrastructure] val WRITE_DIR = "w"
  /** The initial database version. */
  private[infrastructure] val INITIAL_VERSION = Version(0)

  /** Runs the given body within a transaction on db.
    *
    * Reads do NOT need a transaction:
    * http://lists.neo4j.org/pipermail/user/2010-July/004513.html
    *
    * @param db the database which provides the transaction handling
    * @param body the code to be executed
    * @tparam T the result of the transaction
    * @return the result of the body
    */
  def tx[T](db: { def beginTx(): Transaction })(body: => T): T = {
    logger.trace("New DB TX")
    import scala.language.reflectiveCalls
    val tx = db.beginTx()
    try {
      val res = body
      logger.trace("Committing DB TX")
      tx.success()
      res
    } catch {
      case t: Throwable =>
        tx.failure()
        throw t
    } finally {
      tx.close()
    }
  }

  /** Creates a read-only database instance in the given location. */
  private def readOnly(path: Path): GraphDatabaseService =
    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path.toString)
      .setConfig(GraphDatabaseSettings.keep_logical_logs, false.toString)
      .setConfig(GraphDatabaseSettings.read_only, true.toString)
      .newGraphDatabase()

  /** Creates a read-write database instance in the given location. */
  private def readWrite(path: Path): GraphDatabaseService = {
    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path.toString)
      .setConfig(GraphDatabaseSettings.keep_logical_logs, false.toString)
      .setConfig(GraphDatabaseSettings.cache_type, "none")
      .newGraphDatabase()
  }

  /** Creates an empty initial read-write database.
    *
    * Initializes the type-indices.
    *
    * @param dbBaseDir the base directory for all db folders
    * @return an initial read-write database paired with the initial version number
    */
  def createEmptyWriteDb(dbBaseDir: Path): (Version, GraphDatabaseService) = {
    val writeDir = dbBaseDir.resolve(WRITE_DIR).resolve(INITIAL_VERSION.toString)
    logger.debug(s"Creating empty database in $writeDir")
    val db = readWrite(writeDir)
    logger.debug(s"Copying initial configuration.")
    val sourceConfig = dbBaseDir.resolve(Configuration.configJar).toFile
    val targetConfig = writeDir.resolve(Configuration.configJar).toFile
    Try {
      FileUtils.copyFile(sourceConfig, targetConfig)
    } match {
      case Success(_) => logger.debug("Copied initial configuration.")
      case Failure(e) => logger.warn(s"Could not copy initial configuration $sourceConfig to $targetConfig")
    }
    (INITIAL_VERSION, db)
  }

  /** Loads the newest read-ony database from the given location.
    *
    * @param dbBaseDir the base directory for all db folders
    * @return the newest read-ony database paired with its version number, or None if none exists
    */
  def loadNewestDb(dbBaseDir: Path): Option[(Version, GraphDatabaseService)] = {
    val dbNrOption = findNewestReadDbNumber(dbBaseDir)
    dbNrOption.map { dbNr => (dbNr, readOnly(dbBaseDir.resolve(READ_DIR).resolve(dbNr.toString))) }
  }

  /** Copies the read-write database in version 'currentDbId' as the new read-only database.
    *
    * EmbeddedReadOnlyGraphDatabase currently logs warnings:
    * Aug 21, 2012 12:50:36 PM org.neo4j.kernel.impl.nioneo.store.PersistenceWindowPool logWarn
    * WARNING: [/tmp/bf_1345546235196/r/1/neostore.relationshipstore.db] Unable to memory map
    *
    * Which is okay according to: https://groups.google.com/d/msg/neo4j/VcdnrohUm3Y/XUHSzJlKHTcJ
    *
    * @param dbBaseDir the base directory for all db folders
    * @param currentDbId the version of the database to be copied
    * @return the new read-only database paired with its version number
    */
  def createReadFromWriteDb(dbBaseDir: Path, currentDbId: Version): (Version, GraphDatabaseService) = {
    val writeDbFolder = dbBaseDir.resolve(WRITE_DIR).resolve(currentDbId.toString)
    val readDbFolder = dbBaseDir.resolve(READ_DIR).resolve(currentDbId.toString)
    logger.debug(s"Copy write to read db ($writeDbFolder -> $readDbFolder)")
    FileUtils.copyDirectory(writeDbFolder.toFile, readDbFolder.toFile)
    logger.trace(s"Copied write to read db ($writeDbFolder -> $readDbFolder)")
    (currentDbId, readOnly(readDbFolder))
  }

  /** Copies the read-only database in version 'currentDbId' as the new read-write database with version 'currentDbId+1'.
    *
    * @param dbBaseDir the base directory for all db folders
    * @param currentDbId the version of the database to be copied
    * @return the new read-write database paired with its version number
    */
  def createWriteFromReadDb(dbBaseDir: Path, currentDbId: Version): (Version, GraphDatabaseService) = {
    val readDbFolder = dbBaseDir.resolve(READ_DIR).resolve(currentDbId.toString)
    val newDbId = currentDbId.next
    val writeDbFolder = dbBaseDir.resolve(WRITE_DIR).resolve(newDbId.toString)
    logger.debug(s"Copy read to write db ($readDbFolder -> $writeDbFolder)")
    FileUtils.copyDirectory(readDbFolder.toFile, writeDbFolder.toFile)
    logger.trace(s"Copied read to write db ($readDbFolder -> $writeDbFolder)")
    (newDbId, readWrite(writeDbFolder))
  }

  /** Returns the newest (greatest) version in the read sub folder, if any exists.
    * @param dbBaseDir the base directory for all db folders
    * @return the newest version in the read sub folder
    */
  def findNewestReadDbNumber(dbBaseDir: Path): Option[Version] = {
    def parse(in: String): Option[Int] = Try(in.trim.toInt).toOption

    val folder = dbBaseDir.resolve(READ_DIR)
    if (Files.exists(folder)) {
      val directoryNumbers = folder.toFile.listFiles().filter(d => d.isDirectory).map(d => parse(d.getName)).flatten
      if (directoryNumbers.isEmpty) None
      else Some(new Version(directoryNumbers.max))
    } else {
      None
    }
  }

  /** Deletes all files in the write directory. */
  def cleanWriteDirectory(dbBaseDir: Path) {
    FileUtils.cleanDirectory(dbBaseDir.resolve(WRITE_DIR).toFile)
  }
}
