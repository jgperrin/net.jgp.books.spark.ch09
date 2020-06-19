package net.jgp.books.spark.ch09.x.utils

import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy

/**
 * Simple annotation to extend and ease the Javabean metadata when
 * converting to a Spark column in a dataframe.
 *
 * @author rambabu.posa
 */
@Retention(RetentionPolicy.RUNTIME)
trait SparkColumnScala {
  /**
   * The name of the column can be overriden.
   */
  def name: String

  /**
   * Forces the data type of the column
   */
  def `type`: String

  /**
   * Forces the required/nullable property
   */
  def nullable: Boolean
}

