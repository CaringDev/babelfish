package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.infrastructure.Babelfish
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import ch.fhnw.imvs.babelfish.util.Info

/** Servlet for access to the Babelfish system status. */
class StatusAccess(bf: Babelfish) extends DefaultJsonServlet {
  private val gb = 1024 * 1024 * 1024f

  get() {
    val totalMem = Runtime.getRuntime.totalMemory()
    val usedMem = totalMem - Runtime.getRuntime.freeMemory()
    val maxMem = Runtime.getRuntime.maxMemory()
    Status(Info.version, usedMem / gb, totalMem / gb, maxMem / gb)
  }

  get("/statistics") {
    val (numNodes, numEdges, dbVersion) = Await.result(bf.runReadJob((db) => {
      (db.schema.nodes.map(n => n.name -> db.nodes(n).size).toMap,
        db.schema.edges.filterNot(_.isId).map(e => e.name -> db.edges(e).size).toMap,
        db.version)
    }), Duration.Inf)
    val config = bf.configuration(dbVersion)
    Statistics(dbVersion.v, config.version, config.configBFversion, config.configurationFile.toString, numNodes, numEdges)
  }
}

case class Status(
  bfVersion: String,
  usedMemoryGB: Float,
  totalMemoryGB: Float,
  maxMemoryGB: Float)

case class Statistics(
  dbVersion: Int,
  configVersion: String,
  configBFversion: String,
  configJarPath: String,
  numberOfNodes: Map[String, Int],
  numberOfEdges: Map[String, Int])