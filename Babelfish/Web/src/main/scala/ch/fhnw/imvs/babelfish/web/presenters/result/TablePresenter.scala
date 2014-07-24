package ch.fhnw.imvs.babelfish.web.presenters.result

import ch.fhnw.imvs.babelfish.dsl.{ Timing, TableResult }

case class TablePresenter(
  columnNames: Vector[String],
  columnTypes: Vector[String],
  rows: Vector[Vector[Any]],
  timings: Seq[Timing],
  resultType: String = "Table") extends ResultPresenter

object TablePresenter {
  def apply[T](t: TableResult): TablePresenter = {
    val start = System.currentTimeMillis()
    val typeNames = t.table.columnTypes.map(_.name)
    val values = t.table.values.map {
      _.map{
        case None => null // Special case for empty table cells
        case o    => o
      }
    }
    val end = System.currentTimeMillis()
    TablePresenter(t.table.columnNames, typeNames, values, t.timings :+ Timing(Timing.present, end - start))
  }
}
