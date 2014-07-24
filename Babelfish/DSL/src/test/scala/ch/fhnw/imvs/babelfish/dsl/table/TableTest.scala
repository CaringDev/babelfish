package ch.fhnw.imvs.babelfish.dsl.table

import ch.fhnw.imvs.babelfish.dsl.core.Extractable.BigDecimalExtractor
import org.scalatest.{ Matchers, FeatureSpec }

/*
~test-only ch.fhnw.imvs.babelfish.dsl.table.TableTest
*/
class TableTest extends FeatureSpec with Matchers {
  feature("Table representation") {
    scenario("Double column") {
      val empty = Table(Vector(), Vector(), Vector())
      InMemorySqlProcessor.evaluate(
        empty,
        "SELECT * FROM (VALUES(1.0))") should be (Table(Vector("C1"), Vector(BigDecimalExtractor), Vector(Vector(1.0))))
    }
  }
}
