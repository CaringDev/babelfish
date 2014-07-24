package ch.fhnw.imvs.babelfish.web.presenters

import ch.fhnw.imvs.babelfish.web.JobInfo
import scala.util.{ Success, Failure }
import org.joda.time.DateTimeUtils
import ch.fhnw.imvs.babelfish.dsl.{ TableResult, PathResult }

case class JobPresenter(
  id: Long,
  query: String,
  startedAt: Long,
  finished: Boolean,
  resultType: Option[String],
  finishedAt: Option[Long],
  status: String)

object JobPresenter {
  def apply(id: Long, job: JobInfo): JobPresenter = {
    val query = job.queryString
    val startedAt = DateTimeUtils.getInstantMillis(job.jobStartedAt)
    val finished = job.jobFinishedAt.isDefined
    val finishedAt = job.jobFinishedAt.map(DateTimeUtils.getInstantMillis(_))
    val (resultType, statusMessage) = job.future.value match {
      case None                 => None -> "Job in progress."
      case Some(Failure(error)) => Some("Error") -> error.getMessage
      case Some(Success((result, _))) => result match {
        case p: PathResult =>
          Some("Path") -> s"Job completed with ${p.paths.size} paths."
        case t: TableResult =>
          Some("Table") -> s"Job completed with table of size ${t.table.columnNames.size} x ${t.table.values.size}."
      }
    }
    JobPresenter(id, query, startedAt, finished, resultType, finishedAt, statusMessage)
  }
}