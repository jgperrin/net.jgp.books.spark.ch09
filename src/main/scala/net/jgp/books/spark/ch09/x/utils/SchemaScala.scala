package net.jgp.books.spark.ch09.x.utils

import java.io.Serializable
import java.util.HashMap
import org.apache.spark.sql.types.StructType

/**
 * Stores the Spark schema as well as extra information we cannot add to the
 * (Spark) schema.
 *
 * @author rambabu.posa
 */
@SerialVersionUID(2376325490075130182L)
class SchemaScala extends Serializable {
  val columns = new HashMap[String, SchemaColumn]()
  var structSchema: StructType = new StructType()

  def getSparkSchema: StructType = structSchema

  /**
   * @param structSchema
   * the structSchema to set
   */
  def setSparkSchema(structSchema: StructType): Unit = {
    this.structSchema = structSchema
  }

  def add(col: SchemaColumn): Unit = {
    columns.put(col.getColumnName, col)
  }

  def getMethodName(columnName: String): String =
    columns.get(columnName).getMethodName

}