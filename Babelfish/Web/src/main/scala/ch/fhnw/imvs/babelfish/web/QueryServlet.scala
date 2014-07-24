package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.infrastructure.Babelfish
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web.util.GraphViz
import org.scalatra._

/** Servlet for job execution, result retrieval and job overview. */
class QueryServlet(bf: Babelfish) extends DefaultJsonServlet with Logging {

  // default settings:
  private val defaultAggregation = true
  private val defaultLayout = GraphViz.DOT

  val qm: QueryManager = new QueryManager(bf)

  /** List all jobs. */
  get("/") { qm.allJobs() }
  get("/list") { qm.allJobs() }
  /** list job with 'jobId' */
  get("/list/:jobId") { withLongParam("jobId"){ id => Ok(qm.job(id)) } }

  /** Execute a dsl query string. */
  post("(?s)/execute/(.*)".r) {
    val q = multiParams("captures").head
    val result = qm.execute(q)
    Ok(result)
  }

  /** Execute a dsl query string - GET version for browser convenience. */
  get("(?s)/execute/(.*)".r) {
    val q = multiParams("captures").head
    val result = qm.execute(q)
    Ok(result)
  }

  /** Retrieve result of the job with 'jobId' in default format. */
  get("/results/:jobId") {
    fetchResult()
  }

  /** Retrieve result of the job with 'jobId' in a format defined by 'ext'. */
  get("/results/:jobId.:ext") {
    fetchResult()
  }

  private def fetchResult() = {
    withLongParam("jobId") { jobId =>
      val ext = params.getOrElse("ext", defaultExt)

      val aggregated = params.getOrElse("aggregated", defaultAggregation).toString.toBoolean

      val layout = params.getOrElse("layout", defaultLayout).toString.toLowerCase match {
        case GraphViz.DOT.name  => GraphViz.DOT
        case GraphViz.SFDP.name => GraphViz.SFDP
        case x                  => throw new IllegalArgumentException(s"Input parameter 'layout' has to be valid GraphViz layout name (was $x).")
      }

      val (response: ActionResult, mediaType: Option[String]) = qm.result(jobId, ext, aggregated, layout)
      mediaType.foreach { contentType = _ }
      response
    }
  }

  /** cancel a job */
  delete("/cancel/:jobId") {
    withLongParam("jobId") { jobId =>
      qm.cancelJob(jobId)
      Ok()
    }
  }
  /** cancel a job - GET version for browser convenience. */
  get("/cancel/:jobId") {
    withLongParam("jobId") { jobId =>
      qm.cancelJob(jobId)
      Ok()
    }
  }

  /** cancel all jobs (and its GET version) */
  delete("/cancel_all") { qm.cancelAllJobs() }
  get("/cancel_all") { qm.cancelAllJobs() }

  /** the number of alive jobs (in cache) */
  get("/count/alive") { qm.aliveJobCount }

  /** the total number of jobs */
  get("/count/all") { qm.totalJobCount }
}