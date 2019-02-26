package net.jgp.books.spark.ch09.x.utils;

import java.io.Serializable;

public class SchemaColumn implements Serializable {
  private static final long serialVersionUID = 9113201899451270469L;
  private String methodName;
  private String columnName;

  /**
   * @return the methodName
   */
  public String getMethodName() {
    return methodName;
  }

  /**
   * @param methodName
   *          the methodName to set
   */
  public void setMethodName(String method) {
    this.methodName = method;
  }

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @param columnName
   *          the columnName to set
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
}
