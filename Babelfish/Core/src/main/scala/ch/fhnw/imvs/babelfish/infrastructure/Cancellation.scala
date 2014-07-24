package ch.fhnw.imvs.babelfish.infrastructure

import scala.util.control.NoStackTrace

trait Killable { def kill() }

/** This object is thrown in the read job thread when it gets cancelled. */
object CancelException extends RuntimeException with NoStackTrace

/** Handle to a job which allows to request cancellation. */
trait JobController {
  def cancel()
}

/** Allows a running job to query whether cancellation was requested.
  *
  * A running job should periodically check if it should cancel.
  */
trait JobCallback {
  def isCancelled: Boolean
}