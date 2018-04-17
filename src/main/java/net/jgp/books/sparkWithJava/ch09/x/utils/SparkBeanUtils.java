package net.jgp.books.sparkWithJava.ch09.x.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jgp
 */
public class SparkBeanUtils {
  private static transient Logger log = LoggerFactory
      .getLogger(SparkBeanUtils.class);

  public static Schema getSchemaFromBean(Class<?> c) {
    Schema schema = new Schema();
    SchemaColumn col;
    List<StructField> sfl = new ArrayList<>();

    Method[] methods = c.getDeclaredMethods();
    for (int i = 0; i < methods.length; i++) {
      Method method = methods[i];
      String methodName = method.getName();
      if (methodName.toLowerCase().startsWith("get") == false) {
        continue;
      }

      col = new SchemaColumn();
      col.setMethodName(methodName);

      // We have a public method starting with get
      String columnName;
      DataType dataType;
      boolean nullable;
      SparkColumn sparkColumn = method.getAnnotation(SparkColumn.class);
      if (sparkColumn == null) {
        log.debug("No annotation for method {}", methodName);
        columnName = "";
        dataType = getDataTypeFromReturnType(method);
        nullable = true;
      } else {
        columnName = sparkColumn.name();
        log.debug("Annotation for method {}, column name is {}",
            methodName,
            columnName);

        switch (sparkColumn.type().getSimpleName()) {
          case "StringType":
            dataType = DataTypes.StringType;
            break;
          case "BinaryType":
            dataType = DataTypes.BinaryType;
            break;
          case "BooleanType":
            dataType = DataTypes.BooleanType;
            break;
          case "DateType":
            dataType = DataTypes.DateType;
            break;
          case "TimestampType":
            dataType = DataTypes.TimestampType;
            break;
          case "CalendarIntervalType":
            dataType = DataTypes.CalendarIntervalType;
            break;
          case "DoubleType":
            dataType = DataTypes.DoubleType;
            break;
          case "FloatType":
            dataType = DataTypes.FloatType;
            break;
          case "ByteType":
            dataType = DataTypes.ByteType;
            break;
          case "IntegerType":
            dataType = DataTypes.IntegerType;
            break;
          case "LongType":
            dataType = DataTypes.LongType;
            break;
          case "ShortType":
            dataType = DataTypes.ShortType;
            break;
          case "NullType":
            dataType = DataTypes.NullType;
            break;
          default:
            log.debug("Will infer data type from return type for column {}",
                columnName);
            dataType = getDataTypeFromReturnType(method);
        }

        nullable = sparkColumn.nullable();
      }

      String finalColumnName = buildColumnName(columnName, methodName);
      sfl.add(DataTypes.createStructField(finalColumnName, dataType, nullable));
      col.setColumnName(finalColumnName);

      schema.add(col);
    }

    StructType sparkSchema = DataTypes.createStructType(sfl);
    schema.setSparkSchema(sparkSchema);
    return schema;
  }

  /**
   * Returns a Spark datatype from the method, by analyzing the method's return
   * type.
   * 
   * @param method
   * @return
   */
  private static DataType getDataTypeFromReturnType(Method method) {
    String typeName = method.getReturnType().getSimpleName();
    switch (typeName) {
      case "int":
      case "Integer":
        return DataTypes.IntegerType;
      case "long":
      case "Long":
        return DataTypes.LongType;
      case "float":
      case "Float":
        return DataTypes.FloatType;
      case "boolean":
      case "Boolean":
        return DataTypes.BooleanType;
      case "double":
      case "Double":
        return DataTypes.DoubleType;
      case "String":
        return DataTypes.StringType;
      case "Date":
      case "date":
        return DataTypes.DateType;
      case "Timestamp":
        return DataTypes.TimestampType;
      case "short":
      case "Short":
        return DataTypes.ShortType;
      case "Object":
        return DataTypes.BinaryType;
      default:
        log.debug("Using default for type [{}]", typeName);
        return DataTypes.BinaryType;
    }
  }

  private static String buildColumnName(String columnName, String methodName) {
    if (columnName.length() > 0) {
      return columnName;
    }
    columnName = methodName.substring(3);
    if (columnName.length() == 0) {
      return "_c0";
    }
    return columnName;
  }

  public static Row getRowFromBean(Schema schema, Object bean) {
    List<Object> cells = new ArrayList<>();

    String[] fieldName = schema.getSparkSchema().fieldNames();
    for (int i = 0; i < fieldName.length; i++) {
      String methodName = schema.getMethodName(fieldName[i]);
      Method method;
      try {
        method = bean.getClass().getMethod(methodName);
      } catch (NoSuchMethodException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return null;
      } catch (SecurityException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return null;
      }
      try {
        cells.add(method.invoke(bean));
      } catch (IllegalAccessException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IllegalArgumentException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    Row row = RowFactory.create(cells.toArray());
    return row;
  }

}
