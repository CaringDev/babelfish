package ch.fhnw.imvs.babelfish.infrastructure

import ch.fhnw.imvs.babelfish.infrastructure.lowlevel.DbTest._
import ch.fhnw.imvs.babelfish.util.Logging
import org.scalatest.{ Matchers, OptionValues, FunSuite }
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.volatile

/*
~test-only ch.fhnw.imvs.babelfish.infrastructure.InfrastructureTest
*/
class InfrastructureTest extends FunSuite with OptionValues with Logging with Matchers {

  ignore("runReadJob") {
    withBf() { (bf, _) =>
      val future = bf.runReadJob{ db => null }
      Await.ready(future, Duration("100 s"))
      future.isCompleted should be (true)
    }
  }

  test("runWriteJob") {
    withBf() { (bf, _) =>
      @volatile
      var inJobState: Option[ReadWriteDb] = None

      val future = bf.runWriteJob{ db => inJobState = Some(db) }
      Await.ready(future, Duration("100 s"))
      future.isCompleted should be (true)
    }
  }
}
