package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.infrastructure._
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web.presenters.EdgePresenter
import org.scalatra._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Servlet for access to edges. */
class EdgeAccess(bf: Babelfish) extends DefaultJsonServlet with Logging {

  get("/:id") {
    withLongParam("id") { id =>
      edge(id) match {
        case Some(result) => Ok(result)
        case None         => NotFound(s"Could not find edge with id $id.")
      }
    }
  }

  private def edge(id: Long): Option[EdgePresenter] = {
    logger.trace(s"Called: getEdge(id=$id)")
    Await.result(bf.runReadJob{ db =>
      val (privateId, forward) = edgeIdAndDirection(id)
      val edge = db.findEdgeById(privateId)
      logger.trace(s"Edge retrieved: $edge")
      edge.map { e => EdgePresenter(db, QueryResultEdge(e, db.versions(e), forward)) }
    }, Duration.Inf)
  }

  private def edgeIdAndDirection(publicEdgeId: Long): (Long, Boolean) = {
    val dbEdgeId = Math.abs(publicEdgeId)
    val direction = publicEdgeId >= 0
    (dbEdgeId, direction)
  }
}
