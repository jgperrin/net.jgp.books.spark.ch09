package net.jgp.books.spark.ch09.x.ds.exif

import org.apache.spark.sql.sources.DataSourceRegister

/**
 * Defines the "short name" for the data source
 *
 * @author rambabu.posa
 */
class ExifDirectoryDataSourceShortnameAdvertiserScala extends ExifDirectoryDataSourceScala with DataSourceRegister {

  override def shortName = "exif"

}