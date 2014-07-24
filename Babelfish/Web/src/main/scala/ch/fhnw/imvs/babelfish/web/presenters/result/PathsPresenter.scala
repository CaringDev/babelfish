package ch.fhnw.imvs.babelfish.web.presenters.result

import ch.fhnw.imvs.babelfish.web.presenters.{ EdgePresenter, NodePresenter, ElementPresenter }
import ch.fhnw.imvs.babelfish.infrastructure.{ DatabaseManager, QueryDb, QueryResultEdge, QueryResultNode }
import ch.fhnw.imvs.babelfish.dsl.core.VersionedExtractType
import ch.fhnw.imvs.babelfish.web.presenters.versions.{ VersionRangePresenter, VersionedValuePresenter }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Versioned
import ch.fhnw.imvs.babelfish.dsl.{ PathResult, Timing }

case class PathsPresenter(
  paths: Vector[List[Long]],
  labels: Vector[Map[String, Seq[Any]]],
  elementInfo: Seq[ElementPresenter],
  timings: Seq[Timing],
  resultType: String = "Paths") extends ResultPresenter

object PathsPresenter {
  def apply(db: QueryDb, pathResult: PathResult) = {
    val start = System.currentTimeMillis()
    val paths = pathResult.paths.map(_._1.path.reverse.map(_.numericId))
    val labels = pathResult.paths.map {
      case (state, _) =>
        state.labels.mapValues {
          case (values, extractable) =>
            extractable match {
              case v: VersionedExtractType[_] =>
                values.map {
                  _.asInstanceOf[Versioned[_]].extract.map {
                    case (vrs, a) => VersionedValuePresenter(VersionRangePresenter(vrs.versions.head), a)
                  }
                }
              case x =>
                values
            }
        }
    }

    val elementInfo = pathResult.paths.flatMap(_._1.path).distinct.map {
      case n: QueryResultNode => NodePresenter(db, n, false)
      case e: QueryResultEdge => EdgePresenter(db, e, false)
    }
    val end = System.currentTimeMillis()
    new PathsPresenter(paths, labels, elementInfo, pathResult.timings :+ Timing(Timing.present, end - start))
  }
}