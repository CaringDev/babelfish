package ch.fhnw.imvs.babelfish.dsl.table

import ch.fhnw.imvs.babelfish.dsl.Timing
import ch.fhnw.imvs.babelfish.dsl.core._
import ch.fhnw.imvs.babelfish.infrastructure.versioning.VersionRangeSet.AllVersions
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Versioned
import scala.collection.immutable.ListSet

/** Table functionality: from().extract().sql() */
trait TableTrails { self: TrailsPrimitives =>
  /** Syntactic sugar: from {...} extract {...} */
  def from[O](tr: Tr[State[Nothing], State[O], Any]): From[O] =
    new From[O](tr)
}

/** Base class for table operations extract() and sql(). */
final class From[O](val tr: Tr[State[Nothing], State[O], Any]) {

  /** Extract a table from traverser 'tr'.
    * @param columns the variableNames
    * @return the table
    */
  def extract(columns: String*): Extr =
    new Extr(env => in => {
      val startExec = System.currentTimeMillis()
      val paths = tr(env)(in).force
      val endExec = System.currentTimeMillis()

      val cols = (if (columns.length > 0) columns else paths.foldLeft(ListSet[String]())((set, p) => set ++ p._1.labels.keys)).toVector.reverse

      val typeMap = scala.collection.mutable.Map[String, Extractable[_]]()

      val rows = paths.foldLeft(Vector[Vector[List[Versioned[Any]]]]()) {
        case (acc, (State(p, _, labels), _)) =>
          val row = cols.map { name =>
            labels.get(name) match {
              case None => List(null)
              case Some((elems, tag)) =>
                typeMap.update(name, tag)
                tag match {
                  case t: Versionable[_]          => elems.map(e => t.extract(e, p.headOption.map(_.validRanges).getOrElse(AllVersions)))
                  case t: VersionedExtractType[_] => elems.map(t.extract(_, p.headOption.map(_.validRanges).getOrElse(AllVersions)))
                }
            }
          }
          acc :+ row
      }
      val table = Table.createFlatTable(cols, cols.map(typeMap(_)), rows)
      val endExtr = System.currentTimeMillis()

      (table, Seq(Timing(Timing.execute, endExec - startExec), Timing(Timing.extract, endExtr - endExec)))
    })
}

/** Anything that produces a table ('Extracted') */
final class Extr(tr: Env => State[Nothing] => (Table, Seq[Timing]))
  extends (Env => State[Nothing] => (Table, Seq[Timing])) {

  def apply(db: Env): (State[Nothing] => (Table, Seq[Timing])) =
    tr(db)

  /** Evaluate an sql query on a table and return the result as another table. */
  def sql(sqlQuery: String = s"select * from ${InMemorySqlProcessor.tableName}"): Extr = {
    new Extr(env => in => {
      val (table, timings) = tr(env)(in)
      val start = System.currentTimeMillis()
      val res = InMemorySqlProcessor.evaluate(table, sqlQuery)
      val end = System.currentTimeMillis()
      (res, timings :+ Timing(Timing.sql, end - start))
    })
  }
}