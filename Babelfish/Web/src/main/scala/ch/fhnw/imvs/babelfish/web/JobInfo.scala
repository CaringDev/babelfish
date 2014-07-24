package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.dsl.DslResult
import ch.fhnw.imvs.babelfish.infrastructure.QueryDb
import java.util.concurrent.atomic.AtomicBoolean
import org.joda.time.DateTime
import scala.concurrent.Future

final class JobInfo(
  val future: Future[(DslResult, QueryDb)],
  val queryString: String,
  val jobStartedAt: DateTime)(
    val cancel: AtomicBoolean,
    var jobFinishedAt: Option[DateTime])
