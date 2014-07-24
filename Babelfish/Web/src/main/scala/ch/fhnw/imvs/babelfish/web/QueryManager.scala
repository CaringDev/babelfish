package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.dsl.core.Env
import ch.fhnw.imvs.babelfish.dsl.table.Table
import ch.fhnw.imvs.babelfish.dsl.{ TableResult, PathResult, TrailsInterpreter }
import ch.fhnw.imvs.babelfish.infrastructure._
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web.presenters.JobPresenter
import ch.fhnw.imvs.babelfish.web.presenters.result.graph.{ GraphML, DotPresenter }
import ch.fhnw.imvs.babelfish.web.presenters.result.{ TablePresenter, PathsPresenter, ErrorPresenter }
import ch.fhnw.imvs.babelfish.web.util.{ MediaType, GraphViz }
import com.google.common.cache.{ Cache, RemovalNotification, RemovalListener, CacheBuilder }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong }
import org.apache.commons.csv.CSVUtils
import org.joda.time.DateTime
import org.neo4j.graphdb.DatabaseShutdownException
import org.scalatra._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Await }
import scala.util.{ Failure, Success, Try }

/** Manages Babelfish job execution and result retrieval. */
class QueryManager(bf: Babelfish) extends Logging {

  private val nextJobId = new AtomicLong(0)

  private val jobCache: Cache[Long, JobInfo] =
    CacheBuilder.newBuilder().softValues().removalListener(new RemovalListener[Long, JobInfo] {
      def onRemoval(notification: RemovalNotification[Long, JobInfo]) {
        Option(notification.getValue).map(_.cancel.set(true))
        logger.info(s"Cancelling job #${notification.getKey}")
      }
    }).build().asInstanceOf[Cache[Long, JobInfo]]

  private val interpreter = new TrailsInterpreter()

  /** Enumerates all jobs with detailed information. */
  def allJobs(): Iterable[JobPresenter] =
    jobCache.asMap().asScala.map { case (jobId, jobInfo) => JobPresenter(jobId, jobInfo) }

  def job(id: Long): Option[JobPresenter] =
    Option(jobCache.getIfPresent(id)).map(JobPresenter(id, _))

  /** Executes the provided query string
    *
    * @param query the dsl query string
    * @return the numeric id of the new job
    */
  def execute(query: String): Long = {
    logger.trace(s"Called: execute(query=$query)")
    val jobId = nextJobId.incrementAndGet()
    val cancel = new AtomicBoolean()
    val future = bf.runReadJob { (db) =>
      val result = interpreter.executeStatement(query, new Env(db)(cancel))
      (result, db)
    }
    val info = new JobInfo(future, query, DateTime.now)(cancel, None)
    future.onComplete(_ => info.jobFinishedAt = Some(DateTime.now))
    jobCache.put(jobId, info)
    jobId
  }

  private def processTableResult(tableResult: TableResult, ext: String): (ActionResult, Option[String]) = {
    val types = tableResult.table.columnNames.zip(tableResult.table.columnTypes).map{ case (n, t) => s"$n: ${t.name}" }.mkString(", ")
    logger.trace(s"Found Table[$types] with ${tableResult.table.values.size} record(s) in ${tableResult.timings.map(_.millis).sum}ms.")
    ext match {
      case "csv" =>
        val csv = csvFromTable(tableResult.table)
        Ok(csv) -> Some(MediaType.TEXT)
      case "js" | "json" =>
        val json = TablePresenter(tableResult)
        Ok(json) -> Some(MediaType.JSON)
      case _ => NotAcceptable(s"Result of type 'Table' cannot be converted to format '$ext'.") -> None
    }
  }

  private def processPathResult(pathResult: PathResult, db: QueryDb, queryString: String, ext: String, aggregated: Boolean, dotLayout: GraphViz.Layout): (ActionResult, Option[String]) = {
    logger.trace(s"Found ${pathResult.paths.size} paths in ${pathResult.timings.map(_.millis).sum}ms.")

    // most of the result types require the dot functionality, so we write this here for convenience...
    val dot = if (aggregated) DotPresenter.aggregated _ else DotPresenter.allPaths _
    val desc = Some(queryString)

    DatabaseManager.tx(db) {
      ext match {
        case "js" | "json" =>
          val json = PathsPresenter(db, pathResult)
          Ok(json) -> Some(MediaType.JSON)
        case "pdf" =>
          val dotData = dot(pathResult, db, desc)
          val pdf = GraphViz.fromDot(dotData, GraphViz.PDF, dotLayout)
          Ok(pdf) -> Some(MediaType.PDF)
        case "svg" =>
          val dotData = dot(pathResult, db, desc)
          val svg = GraphViz.fromDot(dotData, GraphViz.SVG, dotLayout)
          Ok(svg) -> Some(MediaType.SVG)
        case "xml" =>
          val dotData = dot(pathResult, db, desc)
          val xml = GraphViz.fromDot(dotData, GraphViz.SVG, dotLayout)
          Ok(xml) -> Some(MediaType.XHTML)
        case "gv" =>
          val dotData = dot(pathResult, db, desc)
          Ok(dotData) -> Some(MediaType.TEXT)
        case "graphml" =>
          val data = GraphML.fromCVF(pathResult, db, desc)
          Ok(data) -> Some(MediaType.XML)
        case _ => NotAcceptable(s"Result of type 'Paths' cannot be converted to format '$ext'.") -> None
      }
    }
  }

  /** Retrieves a result, waiting for it indefinitely if necessary.
    *
    * @param jobId the id of the job which produces the result
    * @param ext the format extension in which to present the result
    * @param aggregated true if the results should be aggregated (note: this only matters for GraphViz-specific formats)
    * @param dotLayout the GraphViz layout in which to render the result (note: this only matters for GraphViz-specific formats)
    * @return Some ActionResult and optional MediaType if successful, None otherwise
    */
  def result(jobId: Long, ext: String, aggregated: Boolean, dotLayout: GraphViz.Layout): (ActionResult, Option[String]) = { // returns (result, contentType)
    logger.trace(s"Called: result(jobId=$jobId, ext=$ext)")
    val presenterFuture: Option[Future[(ActionResult, Option[String])]] =
      Option(jobCache.getIfPresent(jobId)).map { jobInfo =>
        jobInfo.future.map {
          case (result, db) =>
            result match {
              case t: TableResult => processTableResult(t, ext)
              case p: PathResult  => processPathResult(p, db, jobInfo.queryString, ext, aggregated, dotLayout)
            }
        }
      }

    presenterFuture match {
      case None => NotFound(s"Could not find result for job with id $jobId.") -> None
      case Some(f) =>
        Try(Await.result(f, Duration.Inf)) match {
          case Failure(CancelException) =>
            val error = ErrorPresenter(s"Job was canceled.")
            (Ok(error), None)
          case Failure(e: DatabaseShutdownException) =>
            logger.info(s"Database for job $jobId was shut down.")
            Option(jobCache.getIfPresent(jobId)).map { jobInfo =>
              jobCache.invalidate(jobId)
              val newResId = execute(jobInfo.queryString)
              (TemporaryRedirect(s"/bf/jobs/results/$newResId.$ext?aggregated=$aggregated&layout=$dotLayout"), None)
            }.getOrElse{
              val msg = s"Re-execution of job $jobId failed."
              logger.warn(msg)
              (Ok(msg), None)
            }
          case Failure(e) =>
            val error = ErrorPresenter(s"Failure during result computation: ${e.printStackTrace(); e.getMessage}")
            (Ok(error), None)
          case Success(res) => res
        }
    }
  }

  /** Serialize a Table to csv format. */
  private def csvFromTable(table: Table): String = {
    val headers = table.columnNames
    val headline = CSVUtils.printLine(headers.to[Array])
    val content = table.values.map { row =>
      CSVUtils.printLine(row.map{
        case null => null
        case x    => x.toString
      }.to[Array])
    }.mkString("\n")
    headline + "\n" + content
  }

  def cancelJob(jobId: Long) {
    logger.trace(s"Called: cancelJob(jobId=$jobId)")
    jobCache.invalidate(jobId)
  }

  def cancelAllJobs() {
    logger.trace(s"Called: cancelAllJobs()")
    jobCache.invalidateAll()
  }

  def aliveJobCount: Long = {
    logger.trace(s"Called: aliveJobCount")
    jobCache.size
  }

  def totalJobCount: Long = {
    logger.trace(s"Called: totalJobCount")
    nextJobId.get()
  }
}

