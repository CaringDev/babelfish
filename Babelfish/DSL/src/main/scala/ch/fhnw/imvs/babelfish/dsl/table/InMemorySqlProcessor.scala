package ch.fhnw.imvs.babelfish.dsl.table

import ch.fhnw.imvs.babelfish.dsl.core._
import ch.fhnw.imvs.babelfish.util.Logging
import java.sql._
import org.joda.time.DateTime

object InMemorySqlProcessor extends Logging {

  /** DB Setup and Settings */
  private val jdbcDriverClass = "org.hsqldb.jdbc.JDBCDriver"
  Class.forName(jdbcDriverClass)

  private val dbName = "dsl_sql_module"
  private val jdbcUrl = s"jdbc:hsqldb:mem:$dbName;shutdown=true"

  private val dbUsername = "DSL_SQL_module"
  private val dbPassword = ""

  val tableName = "t1"

  /** Evaluate an SQL query on a Table and return the result as a String.
    *
    * @note Table headers of the form "A.property" can be referred to as "A_property" in the sql query.
    * (All occurrences of "." are replaced by "_" for SQL compatibility.)
    *
    * @note Data will be stored in table 'tableName' for retrieval by sql query.
    *
    * @param table the input table, completely unfolded (only one version range per row)
    * @param sqlQuery the sql query to apply
    * @return the output string
    */
  def evaluate(table: Table, sqlQuery: String): Table = {
    val db = DriverManager.getConnection(jdbcUrl, dbUsername, dbPassword)
    try {
      val names = table.columnNames.map(_.replaceAll("\\.", "_"))
      if (table.values.nonEmpty) {
        createInMemoryTable(db, names, table)
        loadData(db, names, table)
      }
      logger.trace(s"Executing query on sql table: $sqlQuery")
      val statement = db.createStatement()
      val status = statement.execute(sqlQuery)

      // status is true if the first result is a ResultSet, false if it is an update count or there are no results
      if (!status) throw new IllegalStateException("Could not extract query result. (Negative return value of query execution)")
      val result = resultTable(statement.getResultSet)
      statement.close()
      result
    } finally {
      db.close()
    }
  }

  /** Create an empty SQL table with the correct type headers, using the 'db'. */
  private def createInMemoryTable(db: Connection, columnNames: Seq[String], data: Table) {
    val typeNames = data.columnTypes.map(SqlTypeMapping.toSqlTypeHeader)
    assert(columnNames.size == typeNames.size, s"Error in table data: ${columnNames.size} names but ${typeNames.size} types.")

    val headers = (columnNames, typeNames).zipped.map { (name, tpe) =>
      s"$name $tpe"
    }
    val sqlTableCreation = s"create memory table $tableName (${headers.mkString(", ")});"
    logger.trace(sqlTableCreation)
    val creationStmt = db.createStatement()
    creationStmt.execute(sqlTableCreation)
    creationStmt.close()
  }

  /** Load the data from 'table' into the 'db'. */
  private def loadData(db: Connection, columnNames: Seq[String], data: Table) {
    val sqlDataInsertion = s"insert into $tableName (${columnNames.mkString(", ")}) values (${columnNames.map(_ => "?").mkString(", ")});"
    val ps = db.prepareStatement(sqlDataInsertion)
    logger.trace(s"preparing statement: $sqlDataInsertion")

    data.values.foreach { row =>
      row.zipWithIndex.foreach {
        case (field, idx) =>
          val sqlIndex = idx + 1 // sql rows are 1-based

          val colType = data.columnTypes(idx)
          colType match {
            case v: VectorExtractType[_] =>
              val (_, typeName, conv) = SqlTypeMapping.toSql(v.inner)
              field match {
                case data: Vector[Any] =>
                  val sqlConv: Any => Any = conv.getOrElse(identity)
                  val arr = db.createArrayOf(typeName, data.map(d => sqlConv(d).asInstanceOf[AnyRef]).toArray)
                  ps.setArray(sqlIndex, arr)
              }
            case o: OptionExtractType[_] =>
              field match {
                case data: Option[_] =>
                  val (typeEnum, _, conv) = SqlTypeMapping.toSql(o.inner)
                  val sqlConv: Any => Any = conv.getOrElse(identity)
                  ps.setObject(sqlIndex, data.map(sqlConv).getOrElse(null), typeEnum)
              }
            case otherTypeDecl: SimpleExtractType[_] =>
              val (typeEnum, _, conv) = SqlTypeMapping.toSql(otherTypeDecl)
              val sqlValue: Any = (conv.getOrElse(identity): Any => Any)(field)
              typeEnum match {
                case Types.TIMESTAMP => ps.setTimestamp(sqlIndex, new Timestamp(sqlValue.asInstanceOf[DateTime].getMillis))
                case other           => ps.setObject(sqlIndex, sqlValue, typeEnum)
              }
          }
      }
      ps.addBatch()
    }
    ps.executeBatch()
    ps.close()
  }

  /** Extract the resulting data table from the SQL ResultSet 'result'. */
  private def resultTable(result: ResultSet): Table = {
    val meta = result.getMetaData
    val colCount = meta.getColumnCount
    val colIndices = (1 to colCount).toVector
    val columnNames = for (i <- colIndices) yield meta.getColumnLabel(i)

    val data = Vector.newBuilder[Vector[(Any, Versionable[_])]]
    while (result.next) {
      import scala.language.existentials
      data += (for (i <- colIndices) yield {
        meta.getColumnType(i) match {
          case java.sql.Types.ARRAY =>
            val a = result.getArray(i)
            val (inner: SimpleExtractType[_], convert) = SqlTypeMapping.toScala(a.getBaseType)
            val typeMap = new java.util.HashMap[String, Class[_]]() { (meta.getColumnTypeName(i), inner) }
            val arr = a.getArray(typeMap).asInstanceOf[scala.Array[_]]
            val res: Vector[Any] = arr.toVector
            convert.fold(res)(c => res.map(c)) -> Extractable.vectorExtractor(inner)

          case t =>
            val (inner: SimpleExtractType[_], convert) = SqlTypeMapping.toScala(t)
            val res: Any = result.getObject(i, null: Class[_])
            convert.fold(res)(_(res)) -> inner
        }
      })
    }

    val rows = data.result().map(_.map(_._1))
    val types = data.result().map(_.map(_._2)).headOption.getOrElse(Vector())
    Table(columnNames, types, rows)
  }
}
