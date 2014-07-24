package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.infrastructure.DatabaseManager._
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.{ MigrationAPI, DbFactory, Db }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.schema.Schema
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.{ PlainSchemaConfiguration, InternalConfiguration }
import java.nio.file.Path
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference
import org.neo4j.graphdb.GraphDatabaseService
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future }

/** Babelfish is the central graph database read and write API.
  *
  * Design decisions:
  * - Strict command / query responsibility segregation (CQRS)
  * - Read jobs may run in parallel on a read only database
  * - Write jobs are executed one after another using copy on write technique
  *
  * @param dbBaseDir filesystem location of the database folder
  */
class Babelfish private (dbBaseDir: Path, config: Option[InternalConfiguration] = None) extends Killable with Logging {
  private def createReporter(msg: String): Throwable => Unit = (t: Throwable) => logger.error(msg, t)
  def configuration(v: Version): InternalConfiguration = config.getOrElse(new Configuration(dbBaseDir.resolve(READ_DIR), v))

  // Use one single thread to serialize all write actions
  private val writeJobExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(),
    createReporter("error in writeJob"))

  private val readJobExecutor = ExecutionContext.fromExecutorService(
    new ThreadPoolExecutor(1, Runtime.getRuntime.availableProcessors(), 1, TimeUnit.MINUTES, new LinkedBlockingQueue()),
    createReporter("error in readJob"))

  // Every write access must be executed within writeJobExecutor
  private val currentReadDb = new AtomicReference[Option[Db]](None)

  locally { // Initialization
    loadNewestDb(dbBaseDir) match {
      case None => logger.info("Started new Babelfish (no DB yet.)")
      case Some((id, db)) =>
        val result = new Db(db, id, configuration(id))
        currentReadDb.set(Some(result))
        logger.info(s"Babelfish started with existing DB $id.")

    }
  }

  /* Executes the given block on a single thread executor, one after another. */
  private[this] def asWriteJob[T](block: => T): Future[T] = Future { block }(writeJobExecutor)

  /* job(Current ReadDb) ~> Next ReadDb */
  private[this] def runJobOnCopy[T](dbOrConfig: Option[Version], job: (GraphDatabaseService, Version) => T): (T, Version, GraphDatabaseService) = {
    val (writeId, writeDb) = dbOrConfig match {
      case Some(v) => createWriteFromReadDb(dbBaseDir, v)
      case None    => createEmptyWriteDb(dbBaseDir)
    }
    try {
      val result = job(writeDb, writeId)
      writeDb.shutdown()
      val (readId, readDb) = createReadFromWriteDb(dbBaseDir, writeId)
      (result, readId, readDb)
    } catch {
      case e: Throwable => writeDb.shutdown(); throw e
    } finally {
      cleanWriteDirectory(dbBaseDir)
    }
  }

  /** Schedules a migration job
    *
    * Execution of a write job involves the following steps:
    * 1. Copy the current read database to a new migration database with incremented version
    * 1. Run the migration job on that migration database
    * 1. Move the migration database back as the new read database
    *
    * This method returns immediately since the given job is executed asynchronously on a different thread.
    * The result of executing the job will be available in the returned [[scala.concurrent.Future]].
    *
    * NOTE: the job MUST manage transactions itself. (Writing without a TX will fail!)
    *
    * @param job the function to be executed asynchronously
    * @return the future result
    */
  def runMigrationJob(job: MigrationAPI => Unit): Future[Unit] = {
    asWriteJob {
      val readDb = currentReadDb.get().getOrElse(throw new IllegalStateException("Cannot run migration without db."))
      logger.info(s"Starting migration of DB ${readDb.version}")
      val (result, newReadId, newReadDbS) = runJobOnCopy(Some(readDb.version), (db, v) =>
        job(MigrationAPI(new Db(db, v, configuration(v.previous)))))
      val newReadDb = new Db(newReadDbS, newReadId, configuration(newReadId))
      currentReadDb.set(Some(newReadDb))
      readDb.shutdown()
      result
    }
  }

  /** Schedules a job with write access to the database.
    *
    * Execution of a write job involves the following steps:
    * 1. Copy the current read database to a new write database with incremented version
    * 1. Run the write job on that write database
    * 1. Move the write database back as the new read database
    *
    * This method returns immediately since the given job is executed asynchronously on a different thread.
    * The result of executing the job will be available in the returned [[scala.concurrent.Future]].
    *
    * NOTE: the job MUST manage transactions itself. (Writing without a TX will fail!)
    *
    * @param job the function to be executed asynchronously
    * @tparam T the type of the result
    * @return the future result
    */
  def runWriteJob[T](job: ReadWriteDb => T): Future[T] = {
    def wrappedJob = (dbApi: GraphDatabaseService, version: Version) => {
      val db = new Db(dbApi, version, configuration(version.previous))
      val qrwdb = DbFactory.createReadWriteDb(db)
      job(qrwdb)
    }
    asWriteJob {
      val ((result, newReadId, newReadDbS), oldDb) = currentReadDb.get() match {
        case None         => (runJobOnCopy(None, wrappedJob), None)
        case Some(readDb) => (runJobOnCopy(Some(readDb.version), wrappedJob), Some(readDb))
      }
      val newReadDb = new Db(newReadDbS, newReadId, configuration(newReadId))
      currentReadDb.set(Some(newReadDb))
      oldDb.foreach(_.shutdown())
      result
    }
  }

  /** Schedules a job with read access to the database only.
    *
    * This method returns immediately since the given job is executed asynchronously on a different thread.
    * The result of executing the job will be available in the returned [[scala.concurrent.Future]].
    *
    * @param job the function to be executed asynchronously
    * @tparam T the type of the result
    * @return the future result
    */
  def runReadJob[T](job: QueryDb => T): Future[T] =
    currentReadDb.get() match {
      case Some(db) =>
        Future {
          val queryDb = DbFactory.createQueryDb(db)
          DatabaseManager.tx(queryDb)(job(queryDb))
        }(readJobExecutor)
      case None => throw new IllegalStateException("No database found.")
    }

  private def shutdownCurrentDb() {
    val shutDownJob = Future {
      currentReadDb.get().foreach { db =>
        logger.info(s"Shutting down DB ${db.version}.")
        db.shutdown()
        currentReadDb.set(None)
      }
    }(writeJobExecutor)
    Await.ready(shutDownJob, Duration.Inf)
  }

  def kill() {
    shutdownCurrentDb()
  }
}

/** Babelfish factory. */
object Babelfish {

  /** Creates, initializes and returns a Babelfish instance.
    *
    * @param dbBaseDir filesystem location of the database folder
    * @return an initialized Babelfish instance
    */
  def apply(dbBaseDir: Path): Babelfish =
    new Babelfish(dbBaseDir)

  def apply(dbBaseDir: Path, s: Schema): Babelfish =
    new Babelfish(dbBaseDir, Some(new PlainSchemaConfiguration(s)))
}