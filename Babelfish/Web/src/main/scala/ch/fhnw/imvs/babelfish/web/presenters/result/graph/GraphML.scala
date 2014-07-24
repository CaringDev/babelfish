package ch.fhnw.imvs.babelfish.web.presenters.result.graph

import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultEdge, QueryResultNode, QueryResult, QueryDb }
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.web.presenters.IdentityValueReader
import java.io.StringWriter
import scala.annotation.tailrec
import scala.xml.{ MinimizeMode, Node, NodeSeq, XML }
import ch.fhnw.imvs.babelfish.dsl.PathResult

object GraphML extends IdentityValueReader {

  def fromCVF(pathResult: PathResult, db: QueryDb, label: Option[String]): String = {
    val paths = pathResult.paths.map(_._1.path.reverse)
    val nodes = paths.flatten.collect{ case qrn: QueryResultNode => qrn }.toSet
    val colors = 0.0f.to(1f, 1f / paths.size).map(c => f"$c%1.3f 1.000 1.000")

    val graph = <graphml xmlns="http://graphml.graphdrawing.org/xmlns" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
        http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
                  <key id="label" for="all" attr.name="label" attr.type="string"/>
                  <key id="var" for="all" attr.name="var" attr.type="string"/>
                  <key id="weight" for="edge" attr.name="weight" attr.type="int">
                    <default>1</default>
                  </key>
                  <graph id={ db.schema.name } edgedefault="directed" parse.order="nodesfirst">
                    { nodes.map(n => formatNode(n, db)) }
                    { if (hasEdgeStart(paths) || hasEdgeEnd(paths)) <node id="pseudo"/> }
                    { paths.zip(colors).map(cp => formatPath(db, cp._1, cp._2)) }
                  </graph>
                </graphml>
    val writer = new StringWriter()
    XML.write(writer, graph, "utf-8", xmlDecl = true, null, MinimizeMode.Always)
    writer.toString
  }

  private def formatPath(db: QueryDb, path: List[QueryResult[Identifier]], c: String): NodeSeq = {
    def formatHead(path: List[QueryResult[Identifier]]): NodeSeq = (path.length, path.headOption) match {
      case (0, None)                       => NodeSeq.Empty
      case (_, None)                       => throw new IllegalArgumentException("Missing path element for path length > 0")
      case (1, Some(qre: QueryResultEdge)) => formatEdge(db, "pseudo", "pseudo", qre, c)
      case (_, Some(qre: QueryResultEdge)) => formatTail(path.tail, formatEdge(db, "pseudo", id(path.tail.head), qre, c))
      case (_, Some(_: QueryResultNode))   => formatTail(path, NodeSeq.Empty)
    }
    @tailrec
    def formatTail(path: List[QueryResult[Identifier]], soFar: NodeSeq): NodeSeq = (path.length, path.drop(1).headOption) match {
      case (0 | 1, None)                   => soFar
      case (_, None)                       => throw new IllegalArgumentException("Missing path element for path length > 0")
      case (2, Some(qre: QueryResultEdge)) => soFar ++ formatEdge(db, id(path.head), "pseudo", qre, c)
      case (_, Some(qre: QueryResultEdge)) =>
        val current = formatEdge(db, id(path.head), id(path.tail.tail.head), qre, c)
        formatTail(path.tail.tail, soFar ++ current)
      case (_, Some(QueryResultNode(_, _))) => throw new IllegalArgumentException("Path tail must start with edge.")
    }
    formatHead(path)
  }

  private def formatNode(node: QueryResultNode, db: QueryDb): Node =
    <node id={ s"${node.numericId}" }>
      <data key="label">{ db.nodeType(node.id).name }</data>
    </node>

  private def formatEdge(db: QueryDb, from: String, to: String, edge: QueryResultEdge, c: String, p: Int = 1): Node = {
    val edgeId = edge.id
    val (f, t) = if (edge.forward) from -> to else to -> from
    // color=c
    <edge source={ f } target={ t }>
      <data key="label">{ db.edgeType(edgeId).name }</data>
      { if (p != 1) <data key="weight">{ p }</data> }
    </edge>
  }

  private def id(node: QueryResult[Identifier]): String = node match {
    case null => "pseudo"
    case _    => node.numericId.toString
  }

  private def hasEdgeStart(forest: Iterable[List[QueryResult[Identifier]]]) = {
    @tailrec
    def hasEdgeStart(path: List[QueryResult[Identifier]]): Boolean = path.length match {
      case 0 => false
      case 1 => path.head.id.isInstanceOf[EdgeIdentifier]
      case _ => hasEdgeStart(path.tail)
    }
    forest.exists(hasEdgeStart)
  }

  private def hasEdgeEnd(forest: Iterable[List[QueryResult[Identifier]]]): Boolean = forest.exists(_.head.isInstanceOf[QueryResultEdge])
}
