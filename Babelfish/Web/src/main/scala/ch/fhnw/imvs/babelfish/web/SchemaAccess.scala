package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.importer.CsvSchema
import ch.fhnw.imvs.babelfish.infrastructure.Babelfish
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.schema.Schema
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web.presenters.schema._
import ch.fhnw.imvs.babelfish.web.util.GraphViz
import ch.fhnw.imvs.babelfish.web.util.GraphViz.Types
import ch.fhnw.imvs.babelfish.web.util.MediaType._
import org.scalatra._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

/** Servlet for schema access. */
class SchemaAccess(babelfish: Babelfish) extends DefaultJsonServlet with Logging {

  private def schema() = Try {
    Await.result(babelfish.runReadJob(db => db.schema), Duration.Inf)
  }.getOrElse(babelfish.configuration(new Version(0)).schema)

  get("/"){ displaySchemaAs(schema(), defaultExt) }

  get("/:ext"){ displaySchemaAs(schema(), params("ext")) }

  get("/:name.:ext"){
    val s = schema()
    val N = s.name
    params("name") match {
      case N => displaySchemaAs(s, params("ext"))
      case n => NotFound(s"Could not find a schema calles $n, the current schema is $N")
    }
  }

  /** Meta information includes name, description and number of nodes and edges of the schema. */
  get("/meta") { SchemaMetaPresenter(schema()) }

  /** The schema graph is composed of all nodes and all edges. */
  get("/graph") { SchemaGraphPresenter(schema()) }

  get("/nodes") { schema().nodes.map(SchemaNodePresenter(_)) }

  get("/nodes/:nodeName") {
    val nodeName = params("nodeName")
    schema().node(nodeName) match {
      case Some(node) => Ok(SchemaNodePresenter(node))
      case None       => NotFound(s"Could not find a schema node called '$nodeName'.")
    }
  }

  get("/edges") { schema().edges }

  get("/edges/:edgeName") {
    val edgeName = params("edgeName")
    schema().edge(edgeName) match {
      case Some(edge) => Ok(SchemaEdgePresenter(edge))
      case None       => NotFound(s"Could not find a schema edge called '$edgeName'.")
    }
  }

  private def displaySchemaAs(schema: Schema, ext: String): ActionResult = {
    val dot = (tpe: Types) => Ok(GraphViz.fromDot(GraphViz.createDOT(schema), tpe))
    ext.toLowerCase match {
      case "js" | "json" =>
        contentType = JSON
        Ok(SchemaGraphPresenter(schema))
      case "csv" =>
        contentType = TEXT
        Ok(CsvSchema.schemaAsCsv(schema))
      case "pdf" =>
        contentType = PDF
        dot(GraphViz.PDF)
      case "svg" =>
        contentType = SVG
        dot(GraphViz.SVG)
      case "xml" =>
        contentType = XHTML
        dot(GraphViz.SVG)
      case "gv" =>
        contentType = TEXT
        Ok(GraphViz.createDOT(schema))
      case _ => NotFound(s"Could not produce schema in format '$ext'.")
    }
  }
}
