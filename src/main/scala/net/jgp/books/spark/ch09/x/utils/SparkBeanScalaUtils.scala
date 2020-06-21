package net.jgp.books.spark.ch09.x.utils

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.ArrayList
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * The bean utils helps with the creation of the Schema from a bean and with
 * filling a row with the data.
 *
 * @author rambabu.posa
 */
object SparkBeanScalaUtils {
  private var columnIndex = -1

  /**
   * Builds a schema from the bean. The resulting schema is not directly
   * usable by Spark as it is a super set of what is needed.
   *
   * @param beanClass
   * The bean to analyze
   * @return The Schema
   */
  def getSchemaFromBean(beanClass: Class[_]): Schema =  {
    val schema = new Schema
    val sfl = new ArrayList[StructField]
    val methods = beanClass.getDeclaredMethods
    for (i <- 0 until methods.length) {
      val method = methods(i)
      if (!isGetter(method)) //continue //todo: continue is not supported
        sys.exit(1)
      // The method we are working on is a getter
      val methodName = method.getName
      val col = new SchemaColumn
      col.setMethodName(methodName)
      // We have a public method starting with get
      var columnName:String = null
      var dataType: DataType = null
      var nullable = false
      // Does it have specific annotation?
      val sparkColumn = method.getAnnotation(classOf[SparkColumn])
      if (sparkColumn == null) {
        println("No annotation for method {}", methodName)
        columnName = ""
        dataType = getDataTypeFromReturnType(method)
        nullable = true
      } else {
        columnName = sparkColumn.name
        println("Annotation for method {}, column name is {}", methodName, columnName)
        dataType = sparkColumn.`type`.toLowerCase match {
          case "stringtype" | "string" => DataTypes.StringType

          case "binarytype" | "binary" => DataTypes.BinaryType

          case "booleantype" | "boolean" => DataTypes.BooleanType

          case "datetype" | "date" => DataTypes.DateType

          case "timestamptype" | "timestamp" => DataTypes.TimestampType

          case "calendarintervaltype" | "calendarinterval" => DataTypes.CalendarIntervalType

          case "doubletype" | "double" => DataTypes.DoubleType

          case "floattype" | "float" => DataTypes.FloatType

          case "bytetype" | "byte" => DataTypes.ByteType

          case "integertype" | "integer" | "int" => DataTypes.IntegerType

          case "longtype" | "long" => DataTypes.LongType

          case "shorttype" | "short" => DataTypes.ShortType

          case "nulltype" | "null" => DataTypes.NullType

          case _ => getDataTypeFromReturnType(method)
        }
         nullable = sparkColumn.nullable
      }

      val finalColumnName = buildColumnName(columnName, methodName)
      sfl.add(DataTypes.createStructField(finalColumnName, dataType, nullable))
      col.setColumnName(finalColumnName)
      schema.add(col)
    } // end for

    val sparkSchema = DataTypes.createStructType(sfl)
    schema.setSparkSchema(sparkSchema)
    return schema
  }

  /**
   * Builds a row using the schema and the bean.
   */
  def getRowFromBean(schema: Schema, bean: Any): Row = {
    val cells = new ArrayList[AnyRef]
    val fieldName: Array[String] = schema.getSparkSchema.fieldNames
    for (i <- 0 until fieldName.length) {
      val methodName: String = schema.getMethodName(fieldName(i))
      var method: Method = null
      try method = bean.getClass.getMethod(methodName)
      catch {
        case e: NoSuchMethodException =>
          println("The method {} does not exists: {}.", methodName, e.getMessage)
          return null
        case e: SecurityException =>
          println("The method {} raised a security concern: {}.", methodName, e.getMessage)
          return null
      }
      try cells.add(method.invoke(bean))
      catch {
        case e: IllegalAccessException =>
          println("The method {} is not accessible: {}.", methodName, e.getMessage)
          return null
        case e: IllegalArgumentException =>
          // this should never happen as we have check that the getter has no
          // argument, but we never know, right?
          println("The method {} is expecting arguments: {}.", methodName, e.getMessage)
          return null
        case e: InvocationTargetException =>
          println("The method {} raised an invocation target issue: {}.", methodName, e.getMessage)
          return null
      }
    }
    val row: Row = RowFactory.create(cells.toArray)
    row
  }

  /**
   * Build the column name from the column name or the method name. This
   * method should be improved to ensure name unicity.
   */
  private def buildColumnName(columnName: String, methodName: String): String = {
    if (columnName.length > 0) return columnName
    if (methodName.length < 4) { // Very simplistic
      columnIndex += 1
      return "_c" + columnIndex
    }
    val colName = methodName.substring(3)
    if (colName.length == 0) {
      columnIndex += 1
      return "_c" + columnIndex
    }
    colName
  }

  /**
   * Returns a Spark datatype from the method, by analyzing the method's
   * return type.
   *
   * @param method
   * @return
   */
  private def getDataTypeFromReturnType(method: Method): DataType = {
    val typeName = method.getReturnType.getSimpleName.toLowerCase
    typeName match {
      case "int" | "integer"  => DataTypes.IntegerType
      case "long"             => DataTypes.LongType
      case "float"            => DataTypes.FloatType
      case "boolean"          => DataTypes.BooleanType
      case "double"           => DataTypes.DoubleType
      case "string"           => DataTypes.StringType
      case "date"             => DataTypes.DateType
      case "timestamp"        => DataTypes.TimestampType
      case "short"            => DataTypes.ShortType
      case "object"           => DataTypes.BinaryType
      case _                  => DataTypes.BinaryType
    }
  }

  /**
   * Return true if the method passed as an argument is a getter, respecting
   * the following definition:
   * <ul>
   * <li>starts with get</li>
   * <li>does not have any parameter</li>
   * <li>does not return null
   * <li>
   * </ul>
   *
   * @param method
   * method to check
   * @return
   */
  private def isGetter(method: Method): Boolean = {
    if (!method.getName.startsWith("get")) return false
    if (method.getParameterTypes.length != 0) return false
    if (classOf[Unit] == method.getReturnType) return false
    true
  }

}
