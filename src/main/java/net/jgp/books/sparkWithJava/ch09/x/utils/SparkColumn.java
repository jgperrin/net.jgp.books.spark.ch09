package net.jgp.books.sparkWithJava.ch09.x.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.spark.sql.types.DataType;

/**
 * Simple annotation to extend and ease the Javabean metadata when converting to
 * a Spark column in a dataframe.
 * 
 * @author jgp
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

  /**
   * The name of the column can be overriden.
   */
  String name() default "";

  /**
   * Forces the data type of the column
   */
  Class<? extends DataType> type() default DataType.class;

  /**
   * Forces the required/nullable property
   */
  boolean nullable() default true;
}
