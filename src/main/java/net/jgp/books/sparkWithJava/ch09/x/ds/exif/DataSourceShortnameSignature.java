package net.jgp.books.sparkWithJava.ch09.x.ds.exif;

import org.apache.spark.sql.sources.DataSourceRegister;

/**
 * Defines the "short name"
 * 
 * @author jgp
 */
public class DataSourceShortnameSignature
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
