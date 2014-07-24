package ch.fhnw.imvs.babelfish.web.presenters.result.graph

import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultEdge, QueryResultNode, QueryResult, QueryDb }
import ch.fhnw.imvs.babelfish.infrastructure.lowlevel._
import ch.fhnw.imvs.babelfish.web.presenters.IdentityValueReader
import ch.fhnw.imvs.babelfish.web.presenters.result.graph.GraphFactory.WeightedPath
import java.util.Locale
import scala.annotation.tailrec
import ch.fhnw.imvs.babelfish.dsl.PathResult

/** Construct a dot graph from a cyclic version forest. */
object DotPresenter extends IdentityValueReader {

  /** Create dot, including all the paths of the cyclic version forest. */
  def allPaths(pathResult: PathResult, db: QueryDb, label: Option[String]): String = {
    val paths = pathResult.paths.map(_._1.path.reverse)
    val colors = 0.0f.to(1f, 1f / paths.size).map(c =>
      formatLanguageNeutral("%1.3f 1.000 1.000", c.asInstanceOf[AnyRef]))
    val dotForest = preamble(paths, db, label) +
      paths.zip(colors).map(cp => formatPath(db, cp._1, cp._2)).mkString("\n") +
      "}"
    dotForest
  }

  /** Create an aggregated dot graph, using weighted edges. */
  def aggregated(pathResult: PathResult, db: QueryDb, label: Option[String]): String = {
    val paths = pathResult.paths.map(_._1.path.reverse)
    val edges = GraphFactory.mergePaths(paths)
    val pathIds = edges.map(e => e.weightedPaths.map(_.pathId)).flatten.toSet
    val colorMap = pathIds.zipWithIndex.toMap.mapValues(c =>
      formatLanguageNeutral("%1.3f 1.000 1.000", (c.toFloat / pathIds.size).asInstanceOf[AnyRef]))
    val dotForest = preamble(paths, db, label) +
      edges.map { e =>
        val totalWeight = e.weightedPaths.map(_.weight).sum
        val colors = e.weightedPaths.map {
          case WeightedPath(i, weight) =>
            formatLanguageNeutral("%s;%1.5f", colorMap(i), (weight.toFloat / totalWeight).asInstanceOf[AnyRef])
        }.toSeq.sorted.mkString(":")
        formatEdge(db, id(e.from), id(e.to), e.edge, colors, totalWeight)
      }.mkString("\n") +
      "}"
    dotForest
  }

  private def formatLanguageNeutral(format: String, a: AnyRef*) = String.format(Locale.ENGLISH, format, a: _*)

  private def id(node: QueryResult[Identifier]): String = node match {
    case null => "pseudo"
    case _    => node.numericId.toString
  }

  private def preamble(paths: Iterable[List[QueryResult[Identifier]]], db: QueryDb, label: Option[String]): String = {
    val nodes = paths.flatten.collect{ case qrn: QueryResultNode => qrn }.toSet
    "digraph forest {\n" +
      label.map(l => s"""label="${l.replaceAllLiterally("\"", "\\\"")}"${"\n"}""").getOrElse("") +
      "graph [charset=utf8 rankdir=LR balign=left]\n" +
      "node [shape=rect]\n" +
      (if (hasEdgeStart(paths) || hasEdgeEnd(paths)) "pseudo [shape=circle]\n" else "") +
      nodes.map(formatNode(_, db)).mkString("\n") + "\n"
  }

  private def formatPath(db: QueryDb, path: List[QueryResult[Identifier]], c: String): String = {
    def formatHead(path: List[QueryResult[Identifier]]) = (path.length, path.headOption) match {
      case (0, None)                       => ""
      case (_, None)                       => throw new IllegalArgumentException("Missing path element for path length > 0")
      case (1, Some(qre: QueryResultEdge)) => formatEdge(db, "pseudo", "pseudo", qre, c) + "\n"
      case (_, Some(qre: QueryResultEdge)) =>
        val current = formatEdge(db, "end", id(path.tail.head), qre, c) + "\n"
        formatTail(path.tail, new StringBuilder(current))
      case (_, Some(_: QueryResultNode)) => formatTail(path, new StringBuilder())
    }
    @tailrec
    def formatTail(path: List[QueryResult[Identifier]], soFar: StringBuilder): String = (path.length, path.drop(1).headOption) match {
      case (0 | 1, None)                   => soFar.toString()
      case (_, None)                       => throw new IllegalArgumentException("Missing path element for path length > 0")
      case (2, Some(qre: QueryResultEdge)) => soFar.append(formatEdge(db, id(path.head), "pseudo", qre, c) + "\n").toString()
      case (_, Some(qre: QueryResultEdge)) =>
        val current = formatEdge(db, id(path.head), id(path.tail.tail.head), qre, c) + "\n"
        formatTail(path.tail.tail, soFar.append(current))
      case (_, Some(QueryResultNode(_, _))) => throw new IllegalArgumentException("Path tail must start with edge.")
    }
    formatHead(path)
  }

  private def formatNode(node: QueryResultNode, db: QueryDb): String = {
    val identity = identityValues(db, node.id).map{ case (k, v) => s"$k: $v" }.mkString("""<br align="left"/>""")
    s"""${node.numericId} [label=<<B>${db.nodeType(node.id).name}</B><br align="left"/>$identity<br align="left"/>>]"""
  }

  private def formatEdge(db: QueryDb, from: String, to: String, edge: QueryResultEdge, c: String, p: Int = 1): String = {
    val edgeId = edge.id
    val dir = if (edge.forward) s"$from -> $to" else s"$to -> $from"
    formatLanguageNeutral("""%s [label=<%s> color="%s" penwidth="%1.3f"]""", dir, db.edgeType(edgeId).name, c, Math.sqrt(p).asInstanceOf[AnyRef])
  }

  private def hasEdgeStart(paths: Iterable[List[QueryResult[Identifier]]]) = {
    @tailrec
    def hasEdgeStart(path: List[QueryResult[Identifier]]): Boolean = path.length match {
      case 0 => false
      case 1 => path.head.isInstanceOf[QueryResultEdge]
      case _ => hasEdgeStart(path.tail)
    }
    paths.exists(hasEdgeStart)
  }

  private def hasEdgeEnd(forest: Iterable[List[QueryResult[Identifier]]]): Boolean = forest.exists(_.head.isInstanceOf[QueryResultEdge])
}
