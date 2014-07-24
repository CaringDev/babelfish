package ch.fhnw.imvs.babelfish.util

import ch.fhnw.imvs.babelfish.infrastructure.Killable
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.util.{ Try, Success, Failure }

/** Listens for newly created files within a given directory and calls a handler function on detection of such a file.
  *
  * Once created immediately starts observing the given directory and will continue to run until `kill`ed.
  * @param path the path to a folder to observe
  * @param ext the extension (e.g. ".zip") to monitor
  * @param resGen a function to create a suitable result
  * @param listener the function to call once a new file is detected
  */
class FileSystemWatcher[F](path: Path, ext: String, resGen: Path => F)(listener: F => Unit) extends Killable with Logging {
  private val exec = Executors.newSingleThreadExecutor()
  private val watcher = FileSystems.getDefault.newWatchService()
  private val watchKey = path.register(watcher, ENTRY_CREATE)

  exec.submit(new Runnable {
    import scala.collection.JavaConverters._
    def run() {
      @tailrec
      def loop() {
        val watched = Try(watcher.take).map { wKey =>
          val events = wKey.pollEvents()
          events.asScala.foreach {
            case event: WatchEvent[Path] => handleEvent(path.resolve(event.context()), listener, 0)
            case otherEvent              => logger.warn(s"Received wrong event. ${otherEvent.kind().name}")
          }
          wKey.reset()
        }
        watched match {
          case Success(_)                              => loop()
          case Failure(e: ClosedWatchServiceException) => logger.info(s"Watcher for $path quit.")
          case Failure(e)                              => logger.warn(s"Watcher for $path quit.", e)
        }
      }
      loop()
    }
  })

  def kill() {
    watchKey.cancel()
    watcher.close()
    exec.shutdownNow()
  }

  val MAX_RETRIES = 6
  val SLEEP_TIME = 10 * 1000 // wait for 10 seconds (file write should be finished by then)

  private def handleEvent(path: Path, listener: F => Unit, retryCount: Int) {
    if (!Files.isDirectory(path) && path.toString.endsWith(ext) && retryCount < MAX_RETRIES /* Wait for max 1 minute */ ) {
      logger.debug(s"Detected $path, waiting for ${SLEEP_TIME / 1000.0}s.")
      Thread.sleep(SLEEP_TIME)

      Try(resGen(path)) match {
        case Success(f) =>
          Try(listener(f)) match {
            case Failure(e) => logger.warn("Listener should never fail", e)
            case Success(_) => // okay
          }
        case Failure(error) => {
          // we could not read the file. This most likely means that someone is still modifying it.
          // we do nothing and hope that there will be another ENTRY_MODIFY event later on.
          // Alternatively, we could print something like this, if we're willing to take some spam:
          logger.debug(s"File $path detected by file system watcher. Not triggering any action because it is not a valid $ext file.")
          handleEvent(path, listener, retryCount + 1)
        }
      }
    } else if (retryCount >= MAX_RETRIES) {
      logger.error(s"Giving up on $path, retry count exceeded.")
    }
  }
}
