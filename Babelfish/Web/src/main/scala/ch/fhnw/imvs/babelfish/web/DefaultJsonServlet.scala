package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web.util.MediaType.{ CONTENT_TYPE, JSON }
import ch.fhnw.imvs.babelfish.web.util.{ EnumSerializer, DateTimeSerializer }
import javax.servlet.ServletConfig
import org.json4s.{ DefaultFormats, Formats }
import org.scalatra.json.{ JValueResult, JacksonJsonSupport }
import org.scalatra.{ BadRequest, ActionResult, ScalatraServlet }
import scala.util.{ Failure, Success, Try }

/** A default Scalatra Servlet with JSON serialisation support.
  *
  * Extend this if you want straight-forward serialisation to JSON and XML.
  */
trait DefaultJsonServlet extends ScalatraServlet with JacksonJsonSupport with JValueResult with Logging {

  protected implicit val jsonFormats: Formats = DefaultFormats.withBigDecimal + DateTimeSerializer + EnumSerializer

  // default format for all kind of content conversions throughout the web module
  protected val defaultExt = "json"

  before() {
    // the REST client can define the content type, default is JSON
    request.getHeader(CONTENT_TYPE) match {
      case null => contentType = JSON
      case ct   => contentType = ct
    }

    if (allowedRemotes != null) {
      val origin = request.getHeader("Origin")
      origin match {
        case null =>
        case o if o.matches(allowedRemotes) =>
          logger.trace(s"Allowing cross site request from $origin")
          response.addHeader("Access-Control-Allow-Origin", origin)
          response.addHeader("Access-Control-Allow-Credentials", "true")
        case _ => logger.trace(s"Blocked cross site request from $origin")
      }
    }
  }

  override def initialize(config: ServletConfig) {
    super.initialize(config)
    allowedRemotes = config.context.getInitParameter("AllowedRemotes")
  }

  def withLongParam(name: String)(body: Long => ActionResult): ActionResult = {
    Try(params(name).toLong) match {
      case Success(id)                       => body(id)
      case Failure(e: NumberFormatException) => BadRequest(s"$name has to be a valid 'Long'.")
      case Failure(t)                        => BadRequest(s"Request failed: ${t.getMessage}")
    }
  }

  var allowedRemotes: String = null
}
