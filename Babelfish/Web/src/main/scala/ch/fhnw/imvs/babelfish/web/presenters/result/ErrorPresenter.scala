package ch.fhnw.imvs.babelfish.web.presenters.result

import ch.fhnw.imvs.babelfish.dsl.Timing

case class ErrorPresenter(message: String, resultType: String = "Error", timings: Seq[Timing] = Seq()) extends ResultPresenter