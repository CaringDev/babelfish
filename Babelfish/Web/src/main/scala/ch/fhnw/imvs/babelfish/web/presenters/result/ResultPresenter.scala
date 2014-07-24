package ch.fhnw.imvs.babelfish.web.presenters.result

import ch.fhnw.imvs.babelfish.dsl.Timing

/** Marks result presenters.
  *
  * @note ResultPresenters should define the String 'resultType' for distinction at client site.
  */
trait ResultPresenter {
  def timings: Seq[Timing]
  def resultType: String
}
