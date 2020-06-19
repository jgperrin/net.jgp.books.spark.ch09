package net.jgp.books.spark.ch09.x.ds.exif

import java.io.Serializable
import java.util.ArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.StructType
import net.jgp.books.spark.ch09.x.extlib.{ExifUtilsScala, PhotoMetadata, PhotoMetadataScala, RecursiveExtensionFilteredLister}
import net.jgp.books.spark.ch09.x.utils.{Schema, SparkBeanScalaUtils}

/**
 * Build a relation to return the EXIF data of photos in a directory.
 *
 * @author rambabu.posa
 */
class ExifDirectoryRelationScala extends BaseRelation with Serializable with TableScan {
  var sqlCtxt: SQLContext = null
  var s: Schema = null
  var photoLister: RecursiveExtensionFilteredLister = null

  override def sqlContext(): SQLContext = sqlCtxt

  def setSqlContext(s: SQLContext): Unit = {
    sqlCtxt = s
  }

  /**
   * Build and returns the schema as a StructType.
   */
  def schema(): StructType = {
    if (s == null)
      s = SparkBeanScalaUtils.getSchemaFromBean(classOf[PhotoMetadataScala])
    s.getSparkSchema()
  }

  def buildScan(): RDD[Row] = {
    println("-> buildScan()")
    schema()
    // I have isolated the work to a method to keep the plumbing code
    //  as simple as possible.
    val table = collectData()
    val sparkContext = sqlCtxt.sparkContext
    import scala.collection.JavaConversions._
    val rowRDD = sparkContext.parallelize(table).map{photo: PhotoMetadata =>
        SparkBeanScalaUtils.getRowFromBean(s, photo)
    }
    rowRDD
  }

  /**
   * Interface with the real world: the "plumbing" between Spark and
   * existing data, in our case the classes in charge of reading the
   * information from the photos.
   *
   * The list of photos will be "mapped" and transformed into a Row.
   */
  private def collectData(): ArrayList[PhotoMetadata] = {
    val photosToProcess = photoLister.getFiles
    val list = new ArrayList[PhotoMetadata]
    var photo: PhotoMetadata = null
    import scala.collection.JavaConversions._
    for (photoToProcess <- photosToProcess) {
      photo = ExifUtilsScala.processFromFilename(photoToProcess.getAbsolutePath)
      list.add(photo)
    }
    list
  }

  def setPhotoLister(p: RecursiveExtensionFilteredLister): Unit = {
    photoLister = p
  }

}

