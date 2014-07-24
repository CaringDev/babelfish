package ch.fhnw.imvs.babelfish.dsl

import ch.fhnw.imvs.babelfish.InternalConfiguration
import ch.fhnw.imvs.babelfish.dsl.core.{ Env, Tr, State }
import ch.fhnw.imvs.babelfish.dsl.table.{ From, Extr }
import ch.fhnw.imvs.babelfish.infrastructure.QueryDb
import ch.fhnw.imvs.babelfish.infrastructure.versioning.Version
import ch.fhnw.imvs.babelfish.util.Logging
import java.io.{ ByteArrayOutputStream, OutputStreamWriter }
import java.nio.charset.StandardCharsets
import scala.tools.nsc.NewLinePrintWriter
import scala.tools.nsc.interpreter.{ Results, IMain }

/** The Interpreter for the trails DSL.
  *
  * Uses the Scala Interpreter to:
  * - Import core DSL dependencies
  * - Import configured DSL dependencies ("high level extensions")
  * - Compile the query to a Scala function
  *
  * Runs the compiled query and returns a DslResult.
  */
class TrailsInterpreter extends Logging {

  class EmbeddedInterpreter(config: InternalConfiguration, val dbVersion: Version) {
    val interpreterOutputStream = new ByteArrayOutputStream()
    val interpreter = new IMain(
      new InterpreterSettings(config),
      new NewLinePrintWriter(
        new OutputStreamWriter(interpreterOutputStream, StandardCharsets.UTF_8),
        true))

    val imports = Seq("ch.fhnw.imvs.babelfish.dsl.core._",
      "ch.fhnw.imvs.babelfish.dsl.core.Tr._",
      "ch.fhnw.imvs.babelfish.dsl.table._",
      "ch.fhnw.imvs.babelfish.infrastructure.versioning._",
      config.dslBase + "._") ++ config.dslImports

    imports.foreach(i => interpreter.interpret(s"import $i"))

    def compileQuery[A](q: String): Env => State[Nothing] => Any = {
      interpreterOutputStream.reset()
      val result = interpreter.interpret(q)
      val interpreterMsg = interpreterOutputStream.toString("UTF-8")

      result match {
        case Results.Error | Results.Incomplete => throw new IllegalArgumentException(interpreterMsg)
        case Results.Success =>
          val interpreterResult: Option[Any] = interpreter.valueOfTerm(interpreter.mostRecentVar)
          interpreterResult match {
            case Some(q: Tr[State[Nothing], State[Any], Any]) => q
            case Some(from: From[Any])                        => from.tr
            case Some(ex: Extr)                               => ex
            case None =>
              val msg = s"Error: Could not extract result from Scala interpreter. Interpreter says:\n $interpreterMsg."
              throw new IllegalArgumentException(msg)
            case Some(x) =>
              val msg = s"Error: Result of unknown type:\n $x\n\nAborting."
              throw new IllegalArgumentException(msg)
          }
      }
    }
  }

  private val interpreters: ThreadLocal[EmbeddedInterpreter] = new ThreadLocal[EmbeddedInterpreter]()

  private def getInterpreterForCurrentThread(db: QueryDb): EmbeddedInterpreter = {
    val interpreterCandidate = interpreters.get()
    if (interpreterCandidate == null || interpreterCandidate.dbVersion != db.version) {
      logger.trace(s"Creating new interpreter for ${Thread.currentThread().getName}:${Thread.currentThread().getId}")
      val newInterpreter = new EmbeddedInterpreter(db.config, db.version)
      interpreters.set(newInterpreter)
      newInterpreter
    } else interpreterCandidate
  }

  /** Executes 'query' on 'db'. */
  def executeStatement(query: String, env: Env): DslResult = {
    logger.debug("Compiling: " + query)
    val startCompile = System.currentTimeMillis()

    val interpreter = getInterpreterForCurrentThread(env.db)

    val result = interpreter.compileQuery(query)
    val compileTiming = Timing(Timing.compile, System.currentTimeMillis() - startCompile)

    def evaluatePaths(query: Tr[State[Nothing], State[Any], Any]): PathResult = {
      logger.debug(s"Executing compiled path query on db...")
      /* Evaluate all results in the current thread. */
      val startExec = System.currentTimeMillis()
      val queryResult = query(env)(State.init).toVector // .toVector forces the resulting stream to evaluate all paths
      val endExec = System.currentTimeMillis()
      new PathResult(queryResult, Seq(compileTiming, Timing(Timing.execute, endExec - startExec)))
    }

    def evaluateTable(ex: Extr): TableResult = {
      logger.debug(s"Executing compiled table query on db...")
      val (queryResult, timings) = ex(env)(State.init)
      new TableResult(queryResult, compileTiming +: timings)
    }

    result match {
      case tr: Tr[State[Nothing], State[Any], Any] => evaluatePaths(tr)
      case extr: Extr                              => evaluateTable(extr)
    }
  }
}
