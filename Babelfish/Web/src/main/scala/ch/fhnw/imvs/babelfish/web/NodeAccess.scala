package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.infrastructure._
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web.presenters.{ IdentityValueReader, NodePresenter }
import org.scalatra._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Servlet for access to nodes. */
class NodeAccess(bf: Babelfish) extends DefaultJsonServlet with Logging with IdentityValueReader {

  get("/:id") {
    withLongParam("id"){ id =>
      node(id) match {
        case Some(result) => Ok(result)
        case None         => NotFound(s"Could not find node with id $id.")
      }
    }
  }

  private def node(id: Long): Option[NodePresenter] = {
    logger.trace(s"Called: node(id=$id)")
    Await.result(bf.runReadJob { db =>
      val node = db.findNodeById(id)
      logger.trace(s"Node retrieved: $node")
      node.map { n => NodePresenter(db, QueryResultNode(n, db.versions(n))) }
    }, Duration.Inf)
  }
}
