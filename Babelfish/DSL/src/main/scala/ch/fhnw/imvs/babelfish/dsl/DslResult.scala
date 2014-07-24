package ch.fhnw.imvs.babelfish.dsl

import ch.fhnw.imvs.babelfish.dsl.core.State
import ch.fhnw.imvs.babelfish.dsl.table.Table

/** The result of one DSL query. */
sealed trait DslResult {
  def timings: Seq[Timing]
}

class TableResult(val table: Table, val timings: Seq[Timing]) extends DslResult
class PathResult(val paths: Vector[(State[Any], Any)], val timings: Seq[Timing]) extends DslResult
