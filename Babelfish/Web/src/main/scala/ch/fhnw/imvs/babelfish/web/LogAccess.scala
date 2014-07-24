package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.util.Logging
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{ Logger => QosLogger }
import ch.qos.logback.core.read.ListAppender
import org.scalatra.Ok
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/** Servlet for access to logs. */
object LogAccess extends DefaultJsonServlet with Logging {

  private lazy val memoryLogger =
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[QosLogger]
      .getAppender("WEB-JSON").asInstanceOf[ListAppender[ILoggingEvent]]

  get("/count") { Ok(memoryLogger.list.size) }

  get("/") { log() }

  /** Return log entries 'from' (including) up to 'to' (excluding) */
  get("/:from-:to") { withLongParam("from") { from => withLongParam("to"){ to => Ok(log(from, to)) } } }

  private def log(from: Long = 0, to: Long = memoryLogger.list.size + 1) = {
    memoryLogger.list.asScala.slice(from.toInt, to.toInt).map { e =>
      Log(e.getLevel.toString, e.getLoggerName, e.getFormattedMessage, e.getTimeStamp)
    }
  }
}

case class Log(level: String, logger: String, message: String, timestamp: Long)
