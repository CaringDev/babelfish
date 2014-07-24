package ch.fhnw.imvs.babelfish.web.util

import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, Schema }
import java.io.{ FileNotFoundException, ByteArrayInputStream, ByteArrayOutputStream }
import scala.sys.process.ProcessLogger
import scala.util.Try

/** GraphViz module to visualize dot or other graph data. */
object GraphViz {
  val minBuild = 20130000.0000
  val minVersion = "2.29.0"

  private def dotNotFound =
    throw new FileNotFoundException(
      s"Requires 'dot' and 'sfdp' executable >= $minVersion on (server) path (see http://graphviz.org/Download.php)")

  private def dotError(e: Int, msg: String) =
    throw new InternalError(s"Error $e while producing schema pdf. $msg")

  import scala.language.implicitConversions

  private implicit def toLogger(stream: ByteArrayOutputStream): ProcessLogger = {
    ProcessLogger(s => stream.write(s.getBytes("utf-8")))
  }

  /** Creates a Graphviz DOT string from a given schema.
    *
    * To create e.g. a PDF from the returned string install Graphviz and execute
    * "dot -Tpdf -O <file containing the produced string>"
    * @param s the schema to create a GraphViz string for
    * @return a string in Graphviz DOT format depicting the schema
    */
  def createDOT(s: Schema): String = {
    s"""digraph "${s.name}" {\n""" +
      s"""label="${s.name}"${"\n"}""" +
      "graph [charset=utf8 rankdir=BT ratio=0.707 balign=left]\n" + // ratio=1/sqrt(2) ~ landscape
      "node [shape=rect] edge [arrowhead=empty]\n" +
      s.nodes.map(node).mkString("\n") + "\n" +
      s.edges.map(edge).mkString("\n") + "\n" +
      "}"
  }

  private def node(node: SchemaNode): String = {
    val props = node.properties.map(p => (if (p.isId) s"<u>${p.name}</u>" else p.name) + s" : ${p.neoType.name}").mkString("""<br align="left"/>""")
    s"""${node.name} [label=<<B>${node.name}</B><br/>$props<br align="left"/>>]"""
  }

  private def edge(e: SchemaEdge): String = {
    val idRef = if (e.isId) "penwidth=2 " else ""
    val props = e.properties.map(_.name).mkString("<br/>")
    val cards = s"taillabel=<<sup>${e.fromCard}</sup>> headlabel=<<sup>${e.toCard}</sup>>"
    s"${e.from.name} -> ${e.to.name} [${idRef}label=<<b>${e.name}</b><br/>$props> $cards]"
  }

  /** Renders graph from dot data.
    *
    * @param dotString the Graphviz DOT string to be rendered
    * @param tpe the output type
    * @param layout the graph layout engine to use
    * @return a byte array containing the layout graph (e.g. a PDF)
    */
  def fromDot(dotString: String, tpe: Types, layout: Layout = DOT): Array[Byte] = {
    val data = new ByteArrayInputStream(dotString.getBytes("utf-8"))
    import scala.sys.process._
    val out = new ByteArrayOutputStream()
    val err = new ByteArrayOutputStream()
    Try { (s"${layout.name} -T${tpe.name}" #< data #> out).!(err) }.map {
      case 0 => out.toByteArray
      case 1 => if (checkDotVersion) {
        dotError(1, out.toString("utf-8"))
      } else {
        dotError(1, s"Please update graphviz to at least $minVersion")
      }
      case e => dotError(e, out.toString("utf-8"))
    } getOrElse dotNotFound
  }

  /** Is 'dot' (with version >= 'minBuild' installed? */
  def checkDotVersion: Boolean = {
    import scala.sys.process._
    val out = new ByteArrayOutputStream()
    val VersionExtractor = """.*\((.*)\)""".r // e.g. dot - graphviz version 2.29.0 (20130109.0545)
    Try { ("dot -V" #> out).!(out) }.map {
      case 0 =>
        out.toString("UTF-8") match {
          case VersionExtractor(v) => v.toDouble >= minBuild
          case _                   => false
        }
      case _ => false
    }.getOrElse(false)
  }

  sealed trait Layout {
    private[web] def name: String
  }

  case object DOT extends Layout {
    private[web] val name = "dot"
  }

  case object SFDP extends Layout {
    private[web] val name = "sfdp"
  }

  sealed trait Types {
    private[web] def name: String
  }
  case object PDF extends Types {
    private[web] val name = "pdf"
  }
  case object SVG extends Types {
    private[web] val name = "svg"
  }
}
