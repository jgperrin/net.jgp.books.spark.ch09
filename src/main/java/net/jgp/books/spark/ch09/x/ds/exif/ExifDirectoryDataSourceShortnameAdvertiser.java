package net.jgp.books.spark.ch09.x.ds.exif;

import org.apache.spark.sql.sources.DataSourceRegister;

/**
 * Defines the "short name" for the data source
 * 
 * @author jgp
 */
public class ExifDirectoryDataSourceShortnameAdvertiser
    extends ExifDirectoryDataSource
    implements DataSourceRegister {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.sql.sources.DataSourceRegister#shortName()
   */
  @Override
  public String shortName() {
    return "exif";
  }

}
