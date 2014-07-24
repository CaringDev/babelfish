import ch.fhnw.imvs.babelfish.importer.CsvImporter
import ch.fhnw.imvs.babelfish.infrastructure.{ Migrator, Babelfish }
import ch.fhnw.imvs.babelfish.util.Logging
import ch.fhnw.imvs.babelfish.web._
import java.nio.file.Paths
import javax.servlet.ServletContext
import org.scalatra.LifeCycle

/** Central entry point for Web Application which starts up all the components */
class ScalatraBootstrap extends LifeCycle with Logging {

  private var babelfish: Babelfish = null

  override def init(context: ServletContext) {

    logger.info("Web application started.")

    val dbDir = Paths.get(context.getInitParameter("dbDir")).toAbsolutePath
    val importDir = Paths.get(context.getInitParameter("importDir")).toAbsolutePath

    logger.info(s"Starting BF in directory $dbDir")
    babelfish = Babelfish(dbDir)

    if (importDir.toFile.exists) {
      CsvImporter(babelfish, importDir)
      Migrator(babelfish, importDir, dbDir)
    } else logger.warn(s"Import path $importDir does not exist.")

    val servlets = Map(
      new QueryServlet(babelfish) -> "/bf/jobs/*",
      new NodeAccess(babelfish) -> "/bf/nodes/*",
      new EdgeAccess(babelfish) -> "/bf/edges/*",
      new SchemaAccess(babelfish) -> "/bf/schema/*",
      Capabilities -> "/bf/capabilities/*",
      LogAccess -> "/bf/log/*",
      new StatusAccess(babelfish) -> "/bf/status/*")

    servlets.foreach {
      case (servlet, path) => context.mount(servlet, path)
    }

    logger.trace(s"${servlets.size} servlets successfully mounted.")
  }

  override def destroy(context: ServletContext) {
    logger.info("Web application shutting down.")
    babelfish.kill()
  }

}
