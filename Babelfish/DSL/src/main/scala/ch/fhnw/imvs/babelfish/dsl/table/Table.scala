package ch.fhnw.imvs.babelfish.dsl.table

import ch.fhnw.imvs.babelfish.dsl.core.{ VersionedExtractType, Versionable, Extractable }
import ch.fhnw.imvs.babelfish.dsl.core.Extractable._
import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ Version, VersionRangeSet, Versioned }
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet._

case class Table(columnNames: Vector[String], columnTypes: Vector[Versionable[_]], values: Vector[Vector[Any]])

object Table {
  /** Create a flat table.
    * The versions are encoded into two separate columns, the "Versioned[]" elements are unpacked.
    */
  def createFlatTable(columnNames: Vector[String], columnTypes: Vector[Extractable[_]], rows: Vector[Vector[List[Versioned[Any]]]]): Table = {
    val n = columnNames :+ "VERSION.FROM" :+ "VERSION.TO"
    val t = columnTypes.map {
      case v: VersionedExtractType[_] => v.versioned
      case o: Versionable[_]          => o
    } :+ IntExtractor :+ IntExtractor

    val vals = rows.flatMap {
      unfold
    }.collect {
      case r if rowVersion(r).nonEmpty =>
        assert(rowVersion(r).versions.length == 1,
          s"Table is not correctly unfolded: row has more than one version: $r")
        val version = rowVersion(r).versions.head
        r.map{ case null => null; case v => v.values.head } :+ version.from.v :+ version.to.v
    }

    assert(n.size == t.size,
      s"# of column titles (${n.size}) must match # of column types ${t.size}")
    assert(vals.isEmpty || n.size == vals.head.size,
      s"# of column titles (${n.size}) must match # of data columns (${vals.head.size})")

    Table(n, t, vals)
  }

  /** Outer product of all the versioned values in a row. */
  private def unfold(row: Vector[List[Versioned[Any]]]): Vector[Vector[Versioned[Any]]] = {
    row match {
      case Vector() => Vector()
      case Vector(a) => a.toVector.flatMap{
        case null => Vector(Vector(null))
        case v    => v.split.map(Vector(_))
      }
      case Vector(head, tail @ _*) =>
        for {
          elem <- head.toVector.flatMap{
            case null => Seq(null)
            case v    => v.split
          }
          sub <- unfold(tail.toVector)
        } yield elem +: sub
    }
  }

  private def rowVersion(row: Vector[Versioned[Any]]): VersionRangeSet =
    row.foldLeft(AllVersions){ case (acc, versioned) => acc.intersect(if (versioned != null) versioned.versions else AllVersions) }

}