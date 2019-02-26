package net.jgp.books.spark.ch09.x.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.StructType;

/**
 * Stores the Spark schema as well as extra information we cannot add to the
 * (Spark) schema.
 * 
 * @author jgp
 */
public class Schema implements Serializable {
  private static final long serialVersionUID = 2376325490075130182L;

  private StructType structSchema;
  private Map<String, SchemaColumn> columns;

  public Schema() {
    columns = new HashMap<>();
  }

  public StructType getSparkSchema() {
    return structSchema;
  }

  /**
   * @param structSchema
   *          the structSchema to set
   */
  public void setSparkSchema(StructType structSchema) {
    this.structSchema = structSchema;
  }

  public void add(SchemaColumn col) {
    this.columns.put(col.getColumnName(), col);
  }

  public String getMethodName(String columnName) {
    return this.columns.get(columnName).getMethodName();
  }

}
