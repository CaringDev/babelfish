package ch.fhnw.imvs.babelfish.web.presenters.result.graph

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.{ Identifier, EdgeIdentifier, NodeIdentifier }
import scala.annotation.tailrec
import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultEdge, QueryResult }

/** Helper object to create a weighted graph from a multigraph. */
object GraphFactory {
  case class WeightedPath(pathId: Int, weight: Int)
  case class Edge(from: QueryResult[Identifier], to: QueryResult[Identifier], edge: QueryResultEdge, weightedPaths: Iterable[WeightedPath])
  def mergePaths(forest: Iterable[List[QueryResult[Identifier]]]): Iterable[Edge] = {

    def edgeify(path: List[QueryResult[Identifier]]): Seq[(QueryResult[Identifier], QueryResult[Identifier], QueryResult[Identifier])] = {
      def edgeifyHead(path: List[QueryResult[Identifier]]): Seq[(QueryResult[Identifier], QueryResult[Identifier], QueryResult[Identifier])] = path.length match {
        case 0                                              => Seq()
        case 1 if path.head.id.isInstanceOf[EdgeIdentifier] => Seq((null, null, path.head))
        case _ if path.head.id.isInstanceOf[EdgeIdentifier] => edgeifyTail(path.tail, Seq((null, path.tail.head, path.head.asInstanceOf[QueryResultEdge])))
        case _ if path.head.id.isInstanceOf[NodeIdentifier] => edgeifyTail(path, Seq())
      }
      @tailrec
      def edgeifyTail(path: List[QueryResult[Identifier]], soFar: Seq[(QueryResult[Identifier], QueryResult[Identifier], QueryResultEdge)]): Seq[(QueryResult[Identifier], QueryResult[Identifier], QueryResultEdge)] = path.length match {
        case 0 | 1 => soFar
        case 2 if path.tail.head.id.isInstanceOf[EdgeIdentifier] => {
          (path.head, null, path.tail.head.asInstanceOf[QueryResultEdge]) +: soFar
        }
        case _ => {
          val edge = path.tail.head
          val end = path.tail.tail.head
          edgeifyTail(path.tail.tail, Seq((path.head, end, edge.asInstanceOf[QueryResultEdge])) ++ soFar)
        }
      }
      edgeifyHead(path)
    }

    val pathParts = forest.filterNot(_.isEmpty).map(edgeify).zipWithIndex.map {
      case (path, idx) => path.groupBy(_._3).map {
        case (edge, parts) => (idx, parts.head, parts.size)
      }
    }.flatten

    // now we have
    // [pathId, (segment), count]

    val edges = pathParts.groupBy(_._2._3).values.map {
      segments =>
        {
          val segment = segments.head._2
          val weightedPaths = segments.map(s => WeightedPath(s._1, s._3))
          Edge(segment._1, segment._2, segment._3.asInstanceOf[QueryResultEdge], weightedPaths)
        }
    }

    // now we have
    // [segment, [pathId, count]]

    edges
  }
}
